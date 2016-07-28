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

/**
 * For want of a better name, the default model generator for data sets
 */
class DefaultInstrumentModelGenerator(val p: InstrumentPriceSource[DataFrame], val f: RiskFactorSource[DataFrame], val m: InstrumentModelSource[Model[_]], val t: Seq[Transformer]) extends InstrumentModelGenerator
    with InstrumentModelGeneratorSources[DataFrame, Model[_]] with Transformable {

  def this() = this(null, null, null, Array[Transformer]())
  //
  // For the implementation of Transformable
  //
  private var transformers = Vector[Transformer]()
  //
  // Source of Market Risk Factor data (features)
  //
  private var factors: RiskFactorSource[DataFrame] = _
  //
  // Source of Instrument prices data (label)
  //
  private var prices: InstrumentPriceSource[DataFrame] = _
  //
  // Destination of generated models
  //
  private var models: InstrumentModelSource[Model[_]] = _

  private val noFactorsMsg = "No risk factors data was found"
  private val noPricesMsg = "No price data found"

  private val appContext = ApplicationContext.getContext

  val sc = ApplicationContext.sc

  /**
   *
   */
  override def riskFactorSource(source: RiskFactorSource[DataFrame]): Unit = {
    validateSource(source)
    factors = source
  }

  override def instrumentPriceSource(source: InstrumentPriceSource[DataFrame]): Unit = {
    validateSource(source)
    prices = source
  }

  override def instrumentModelSource(source: InstrumentModelSource[Model[_]]): Unit = {
    validateSource(source)
    models = source
  }

  override def hasSources: Boolean = {

    (f != null && p != null && m != null)
  }

  override def buildModel(from: LocalDate, to: LocalDate, dsCodes: Seq[String]): Map[String, (Boolean, String)] = {

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

    // Load the risk factor data.  If not data reject all dsCodes
    val factorsDF = f.factors()
    if (factorsDF.count == 0) {
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
   * Add a Transformer to the sequence
   */
  override def add(t: Transformer): Unit = {

    // don't allow a null value to be added
    if (t == null) {
      throw new IllegalArgumentException(s"Cannot add a null value")
    }
    transformers = transformers :+ t
  }

  /**
   * Apply available transformers in sequence
   */
  override def transform(d: DataFrame): DataFrame = {

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

    // get the estimator
    try {
      val e = getModelEstimator(dsCode, trainDF)
      // rename the training dataframe column
      val regressionDF = trainDF.withColumnRenamed(getlabelColumn, "label")
      // fit the data
      val model = e.fit(regressionDF)
      // TEMP TEMP 
      generateMetadata(dsCode, regressionDF, model.asInstanceOf[Model[_]])
      // persist the model
      persistTheModel(dsCode, model)
    } catch {
      case allExceptions: Throwable => return (false, s"Failed to generate a model: ${allExceptions.getMessage}")
    }
  }

  //
  //  Returns a CrossValidator as an estimator
  //
  private def getModelEstimator(dsCode: String, trainDF: DataFrame): Estimator[_] = {

    val assembler = new VectorAssembler()
      .setInputCols(trainDF.columns.diff(Array[String](getkeyColumn, getlabelColumn))) // Remove label column and date
      .setOutputCol("features")

    val lr = new LinearRegression()
    val pipeline = new Pipeline().setStages(Array(assembler, lr))

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.fitIntercept, Array(true, false))
      .addGrid(lr.standardization, Array(true, false))
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .build()

    new CrossValidator()
      .setEstimator(pipeline)
      .setEstimatorParamMaps(paramGrid)
      .setEvaluator(new RegressionEvaluator)
      .setNumFolds(5) // Use 'Magic' value = 5
  }

  //
  // persist the model
  //
  private def persistTheModel(dsCode: String, model: Any): (Boolean, String) = {

    try {
      m.putModel(dsCode, model.asInstanceOf[Model[_]])
    } catch {
      case allExceptions: Throwable => return (false, s"Failed to persist the model: ${allExceptions.getMessage}")
    }
    (true, "Model created")
  }

  private def generateMetadata(dsCode: String, df: DataFrame, model: Model[_]) {

//    val result = model.transform(df).select("label", "prediction")
//    val predictionAndObservations = result.map { row =>
//      (row.get(0).asInstanceOf[Double], row.get(1).asInstanceOf[Double])
//    }
//    val metrics = new RegressionMetrics(predictionAndObservations)
//    println(s"${dsCode}: MSE = ${metrics.meanSquaredError}, Variance = ${metrics.explainedVariance}, R-Squared = ${metrics.r2}")
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