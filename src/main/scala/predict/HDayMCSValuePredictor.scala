package main.scala.predict

import java.time.LocalDate
import org.apache.spark.ml.Model
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import main.scala.application.ApplicationContext
import main.scala.factors.RiskFactorSource
import main.scala.models.InstrumentModelSource
import main.scala.portfolios.PortfolioValuesSource
import main.scala.transform.DoublesOnlyTransformer
import main.scala.transform.HDayVolatilityTransformer
import main.scala.util.Functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField

/**
 * Implement ValuePredictor to return a portfolio value using Monte Carlo simulation with h-day covariance matrix
 */
class HDayMCSValuePredictor(p: PortfolioValuesSource[DataFrame], f: RiskFactorSource[DataFrame], c: CorrelatedSampleGenerator, m: InstrumentModelSource[Model[_]]) extends ValuePredictor {

  val appContext = ApplicationContext.getContext

  val sc = ApplicationContext.sc

  lazy val mcsNumIterations = appContext.getLong("mcs.mcsNumIterations")
  lazy val instrumentColumn = appContext.getString("portfolioHolding.instrumentColumn")
  lazy val valueColumn = appContext.getString("portfolioHolding.valueColumn")

  private val indexColumn = "Index"
  lazy val predictionColumn = "prediction" // TODO: add to context file

  override def predict(pCode: String, at: LocalDate): Array[(Double, Array[(String, Double)])] = {

    if (pCode == null || pCode.isEmpty()) {
      throw new IllegalArgumentException(s"Invalid portfolio code supplied: ${pCode}")
    }

    if (at == null) {
      throw new IllegalArgumentException(s"Invalid valuation date supplied: ${at}")
    }

    // Get a list of instruments with positions at the given date
    val holdings = p.getHoldings(pCode, at)

    // If no holdings at this date then nothing more to do 
    if (holdings.count() == 0) {
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

    // TODO: remove this to a DI implementation
    val correlationFactors = new HDayVolatilityTransformer().transform(
      new DoublesOnlyTransformer().transform(
        f.factors(at.minusYears(1))))

    val correlationFactorsAsMatrix = dfToArrayMatrix(correlationFactors)
    // 
    val correlatedSamples = c.sampleCorrelated(mcsNumIterations, correlationFactorsAsMatrix)

    // Add an index to ensure that the results are matched
    val correlatedSamplesWithIndex = correlatedSamples.zipWithIndex.map(s => s._1 :+ toDouble(s._2))
    val correlatedSamplesWithIndexSchema = new StructType(correlationFactors.schema.toArray :+ StructField(indexColumn, DataTypes.DoubleType))

    val sqlc = new SQLContext(sc)
    //    val correlatedSamplesAsRDDOfRows = sc.parallelize(correlatedSamples.map { a => Row.fromSeq(a) })
    val correlatedSamplesAsRDDOfRows = sc.parallelize(correlatedSamplesWithIndex.map { a => Row.fromSeq(a) })
    //    val correlatedSamplesAsDF = sqlc.createDataFrame(correlatedSamplesAsRDDOfRows, correlationFactors.schema)
    val correlatedSamplesAsDF = sqlc.createDataFrame(correlatedSamplesAsRDDOfRows, correlatedSamplesWithIndexSchema)

    // Map to co-ordinate the results
    var accumulatedResults = Map[Double, Array[(String, Double, Double)]]()

    // For each instrument get the appropriate model and predict against the feature samples
    // Use a for loop initially to restrict the parallelism to the samples dimension

    for (portfolioHolding <- holdingsAsArray) {

      val dsCode = portfolioHolding._1  // to simplify 
      val holding = portfolioHolding._2 // the code

      // Apply the model to the generated samples
      val predictions = dfToArrayMatrix(m.getModel(dsCode).get.transform(correlatedSamplesAsDF).select(predictionColumn, indexColumn))

      // Accumulate the results by index for safety
      accumulatedResults = predictions.map { p => p(1) -> addResultToMap(accumulatedResults, p, dsCode, holding) }(scala.collection.breakOut)
      accumulatedResults = predictions.map { p => p(1) -> addResultToMap(accumulatedResults, p, dsCode, holding) }(scala.collection.breakOut)
    }

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