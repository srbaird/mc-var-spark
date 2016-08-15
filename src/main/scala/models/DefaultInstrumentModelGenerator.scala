package main.scala.models

import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.Model
import org.apache.spark.sql.DataFrame
import main.scala.factors.RiskFactorSource
import main.scala.prices.InstrumentPriceSource
import main.scala.application.ApplicationContext
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.evaluation.RegressionEvaluator
import main.scala.transform.Transformable
import org.apache.spark.ml.Transformer
import java.time.LocalDate
import org.apache.spark.mllib.evaluation.RegressionMetrics
import main.scala.util.Functions._
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.log4j.Logger

/**
 * For want of a better name, the default model generator for data sets
 */
class DefaultInstrumentModelGenerator(val p: InstrumentPriceSource[DataFrame], 
    val f: RiskFactorSource[DataFrame], 
    val m: InstrumentModelSource[Model[_]], 
    val e: main.scala.models.ModelEstimator, 
    val t: Seq[Transformer]) extends InstrumentModelGenerator {

  def this() = this(null, null, null, null, Array[Transformer]())
  //
  // For the implementation of Transformable
  //
  private var transformers = Vector[Transformer]()

  private val noFactorsMsg = "No risk factors data was found"
  private val noPricesMsg = "No price data found"

  val modelLabelColumn = "label"

  private val appContext = ApplicationContext.getContext

  val sc = ApplicationContext.sc
  //
  //
  //
  private val logger = Logger.getLogger(getClass)

  def hasSources: Boolean = {

    (f != null && p != null && m != null && e != null)
  }

  override def buildModel(from: LocalDate, to: LocalDate, dsCodes: Seq[String]): Map[String, (Boolean, String)] = {

    logger.debug(s"Build model for ${dsCodes.length} codes between ${from} and ${to}")
    val emptyString = ""
    if (dsCodes == null || dsCodes.isEmpty || dsCodes.contains(emptyString) || dsCodes.contains(null)) {
      throw new IllegalArgumentException(s"Invalid dsCode supplied ${}")
    }
    //
    // Ensure the dates are in the correct sequence, to-date follows from-date
    //
    if (from != null && to != null && from.compareTo(to) > 0) {
      throw new IllegalArgumentException(s"The from date ${from} exceeded the to date: ${to}")
    }
    //
    // assert that the dependencies have been set
    //
    if (!hasSources) {
      throw new IllegalStateException(s"All dependencies have not been set")
    }

    // Load the risk factor data.  If no data reject all dsCodes
    val factorsDF = f.factors()
    if (factorsDF.count == 0) {
      logger.trace(s"No risk factors found ")
      return dsCodes.foldLeft(Map[String, (Boolean, String)]()) { (map, dsCode) => map + (dsCode -> (false, noFactorsMsg)) }
    }

    val availablePrices = p.getAvailableCodes()
    // Reject dsCodes where no price data exists
    val missingPrices = dsCodes
      .filter { d => !availablePrices.contains(d) }
      .foldLeft(Map[String, (Boolean, String)]()) { (map, dsCode) => map + (dsCode -> (false, noPricesMsg)) }

    val createdModels = dsCodes
      .filter { d => availablePrices.contains(d) }
      .foldLeft(Map[String, (Boolean, String)]()) { (map, dsCode) => map + (dsCode -> buildModelForDSCode(dsCode, from, to)) }

    missingPrices ++ createdModels
  }

  /**
   * Apply available transformers in sequence
   */
  def transform(d: DataFrame): DataFrame = {

    logger.trace(s"Perform transform on ${d} ")
    if (t.isEmpty) {
      d
    } else {
      t.foldLeft(d)((acc, tr) => tr.transform(acc))
    }
  }

  //
  private def validateSource(source: Any) = if (source == null) throw new IllegalArgumentException(s"Invalid supplied source ${}")

  //
  private def buildModelForDSCode(dsCode: String, from: LocalDate, to: LocalDate): (Boolean, String) = {

    logger.trace(s"Build model for '${dsCode}' between ${from} and ${to} ")
    def getPrices: DataFrame = {

      if (to == null && from == null) {

        p.getPrices(dsCode)

      } else if (to == null) {
        p.getPrices(dsCode, from)

      } else {

        p.getPrices(dsCode, from, to)
      }
    }

    def getFactors: DataFrame = {

      if (to == null && from == null) {

        f.factors

      } else if (to == null) {

        f.factors(from)

      } else {

        f.factors(from, to)
      }
    }

    try {
      val trainDF = transform(featureDataFrameForDSCode(dsCode, getPrices, getFactors)) // apply any supplied additional transformations
      return fitModelToTrainingData(dsCode, trainDF)
    } catch {
      case allExceptions: Throwable => return (false, s"Failed to generate a training dataframe: ${allExceptions.getClass.getSimpleName}")
    }
  }

  //
  // Join the dataset code prices to the risk factors on the price key column
  // 
  private def featureDataFrameForDSCode(dsCode: String, p: DataFrame, f: DataFrame): DataFrame = {

    p.join(f, getkeyColumn)
  }

  //
  // Fit a model to the training data
  //
  private def fitModelToTrainingData(dsCode: String, trainDF: DataFrame): (Boolean, String) = {

    logger.trace(s"Fit model for '${dsCode}' ")
    // get the estimator
    try {
      // Assemble all the risk factors into a feature Vector
      val estimatorDF = new VectorAssembler()
        .setInputCols(trainDF.columns.diff(Array[String](getkeyColumn, getlabelColumn))) // Remove label column and date
        .setOutputCol("features")
        .transform(trainDF)
      val estimator = e.get
      // rename the training dataframe columns
      val regressionDF = estimatorDF.withColumnRenamed(getlabelColumn, modelLabelColumn)
      // fit the data
      val model = estimator.fit(regressionDF)
      generateMetadata(dsCode, regressionDF, model.asInstanceOf[Model[_]])
      // persist the model
      persistTheModel(dsCode, model)
    } catch {
      case allExceptions: Throwable => return (false, s"Failed to generate a model: ${allExceptions.getMessage}")
    }
  }

  //
  // persist the model
  //
  private def persistTheModel(dsCode: String, model: Any): (Boolean, String) = {

    logger.trace(s"Persist ${model} for '${dsCode}' ")
    try {
      m.putModel(dsCode, model.asInstanceOf[Model[_]])
    } catch {
      case allExceptions: Throwable => return (false, s"Failed to persist the model: ${allExceptions.getMessage}")
    }
    (true, "Model created")
  }

  private def generateMetadata(dsCode: String, df: DataFrame, model: Model[_]) {

    logger.trace(s"Generate metadata from ${df} for '${dsCode}' ")
    val result = model.transform(df).select("label", "prediction")
    result.show()
    val resultAsArray = dfToArrayMatrix(result).map { row => (row(0), row(1)) }
    val metrics = new RegressionMetrics(sc.parallelize(resultAsArray))
    logger.info(s"${dsCode}: MSE = ${metrics.meanSquaredError}, Variance = ${metrics.explainedVariance}, R-Squared = ${metrics.r2}")
  }

  //
  //
  //
  private def getlabelColumn = appContext.getString("instrumentPrice.valueColumn")

  //
  //
  //
  private def getkeyColumn = appContext.getString("instrumentPrice.keyColumn")

}