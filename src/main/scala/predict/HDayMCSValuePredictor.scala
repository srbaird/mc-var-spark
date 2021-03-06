package main.scala.predict

import java.time.LocalDate
import org.apache.spark.ml.Model
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.broadcast
import main.scala.application.ApplicationContext
import main.scala.factors.RiskFactorSource
import main.scala.models.InstrumentModelSource
import main.scala.portfolios.PortfolioValuesSource
import main.scala.transform.DoublesOnlyTransformer
import main.scala.transform.HDayVolatilityTransformer
import main.scala.util.Functions.dfToArrayMatrix
import main.scala.util.Functions.toDouble
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD

/**
 * Implement ValuePredictor to return a portfolio value using Monte Carlo simulation with h-day covariance matrix
 */
class HDayMCSValuePredictor(p: PortfolioValuesSource[DataFrame],
  f: RiskFactorSource[DataFrame],
  c: CorrelatedSampleGenerator,
  m: InstrumentModelSource[Model[_]])
    extends ValueGenerator {

  val appContext = ApplicationContext.getContext

  val sc = ApplicationContext.sc

  lazy val mcsNumIterations = appContext.getLong("mcs.mcsNumIterations")
  lazy val instrumentColumn = appContext.getString("portfolioHolding.instrumentColumn")
  lazy val valueColumn = appContext.getString("portfolioHolding.valueColumn")
  lazy val predictionColumn = appContext.getString("instrumentModel.predictionColumn")

  lazy val priceValueColumn = appContext.getString("instrumentPrice.valueColumn")
  lazy val priceKeyColumn = appContext.getString("instrumentPrice.keyColumn")

  lazy val indexColumn = "Index"
  //
  //
  //
  private val logger = Logger.getLogger(getClass)

  /**
   *
   */
  override def value(pCode: String, at: LocalDate): Array[(Double, Array[(String, Double)])] = {

    logger.debug(s"Generate a h-day mcs value for '${pCode}' at ${at}")
    if (pCode == null || pCode.isEmpty()) {
      throw new IllegalArgumentException(s"Invalid portfolio code supplied: ${pCode}")
    }

    if (at == null) {
      throw new IllegalArgumentException(s"Invalid valuation date supplied: ${at}")
    }

    // Get a list of instruments with positions at the given date
    logger.trace(s"Get the holdings for the portfolio")
    val holdings = p.getHoldings(pCode, at)

    // If no holdings at this date then nothing more to do 
    if (holdings.count() == 0) {
      logger.trace(s"No holdings, return an empty array")
      return Array[(Double, Array[(String, Double)])]() // 
    }

    // TODO: implement the portfolio holdings as an array rather than a DataFrame
    val holdingsAsArray = holdings.select(instrumentColumn, valueColumn).collect().map { x => (x.getString(0), x.getInt(1)) }

    // If no model exists for any of the instruments then throw an exception
    val availableModels = m.getAvailableModels

    val missingModels = holdingsAsArray.map(t => t._1).filter { dsCode => !availableModels.contains(dsCode) }
    if (missingModels.length > 0) {
      throw new IllegalStateException(s"No model for instruments: ${missingModels.mkString(", ")}")
    }

    // Generate a DataFrame of n samples 
    // For correlation purposes use 1 year of factor data
    // Don't include observations for the given date
    val to = at.minusDays(1)
    val from = to.minusYears(1)

    // TODO: remove this to a DI implementation
    logger.trace(s"Transform the factors to an matrix of doubles")
    val correlationFactors = new HDayVolatilityTransformer().transform(
      new DoublesOnlyTransformer().transform(
        f.factors(from, to)))
    val correlationFactorsAsMatrix = dfToArrayMatrix(correlationFactors)

    logger.trace(s"Generate ${mcsNumIterations} sets of factors")
    val sampleStartTime = System.currentTimeMillis
    val correlatedSamples = c.sampleCorrelated(mcsNumIterations, correlationFactorsAsMatrix)
    val sampleEndTime = System.currentTimeMillis
    logger.info(s"Generating ${mcsNumIterations} sets of factors took ${sampleEndTime - sampleStartTime}(ms)")

    // Add an index to ensure that the results can be correctly accumulated
    val correlatedSamplesWithIndex = correlatedSamples.zipWithIndex.map(s => s._1 :+ toDouble(s._2))
    val correlatedSamplesWithIndexSchema = new StructType(correlationFactors.schema.toArray :+ StructField(indexColumn, DataTypes.DoubleType))

    val sqlc = new SQLContext(sc)
    import sqlc.implicits._

    val correlatedSamplesAsRDDOfRows = sc.parallelize(correlatedSamplesWithIndex.map { a => Row.fromSeq(a) })
    val correlatedSamplesAsDF = sqlc.createDataFrame(correlatedSamplesAsRDDOfRows, correlatedSamplesWithIndexSchema)
    logger.info(s"Factor samples has ${correlatedSamplesAsRDDOfRows.partitions.size} partitions to perform ${mcsNumIterations} iterations")

    val assembler = new VectorAssembler()
      .setInputCols(correlatedSamplesAsDF.columns.diff(Array[String](priceKeyColumn, indexColumn))) // Remove label column and date
      .setOutputCol("features")
    val featuresDF = assembler.transform(correlatedSamplesAsDF)

    // Map to co-ordinate the results
    var accumulatedResults = Map[Double, Array[(String, Double, Double)]]()

    // For each instrument get the appropriate model and predict against the feature samples
    // Use a for loop initially to restrict the parallelism to the samples dimension

    for (portfolioHolding <- holdingsAsArray) {

      val dsCode = portfolioHolding._1 // to simplify 
      val holding = portfolioHolding._2 // the code

      logger.trace(s"Generate sample predictions for '${dsCode}'")

      // Apply the model to the generated samples
      val modelStartTime = System.currentTimeMillis()
      val model = m.getModel(dsCode).get
      val modelEndTime = System.currentTimeMillis()
      logger.debug(s"Loading the model took ${modelEndTime - modelStartTime}(ms)")

      val predictionStartTime = System.currentTimeMillis()
      val predictions = model.transform(featuresDF).select(predictionColumn, indexColumn)
        .map { row:Row => Array[Double](row.getDouble(0), row.getDouble(1)) }.collect()

      val predictionEndTime = System.currentTimeMillis()

      logger.info(s"Predicting '${dsCode} for ${mcsNumIterations}' iterations took ${predictionEndTime - predictionStartTime}(ms)")

      // Accumulate the results by index
      accumulatedResults = predictions.map { p => p(1) -> addResultToMap(accumulatedResults, p, dsCode, holding) }(scala.collection.breakOut)
    }
    // Filter the accumulated results 
    accumulatedResults.values.map(r => (r.foldLeft(0D) { (acc, t) => acc + t._2 * t._3 }, r.map(a => (a._1, a._2)))).toArray
  }

  //
  //  Accumulate the results of a dsCode
  //
  private def addResultToMap(map: Map[Double, Array[(String, Double, Double)]], p: Array[Double], dsCode: String, holding: Double): Array[(String, Double, Double)] = {

    val index = p(1) // Index is expected to be the second element in the array
    val prediction = p(0) // Prediction value is expected to be the first element in the array
    val indexEntry = if (map.contains(index)) map(index) else Array[(String, Double, Double)]()
    indexEntry :+ (dsCode, prediction, holding)
  }
}