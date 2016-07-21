package test.scala.predict

import java.time.LocalDate
import org.apache.commons.math3.random.ISAACRandom
import main.scala.application.ApplicationContext
import main.scala.factors.RiskFactorSourceFromFile
import main.scala.models.InstrumentModelSourceFromFile
import main.scala.portfolios.PortfolioValuesSourceFromFile
import main.scala.predict.CholeskyCorrelatedSampleGenerator
import main.scala.predict.HDayMCSValuePredictor
import main.scala.predict.RandomDoubleSourceFromRandom
import test.scala.application.SparkTestBase
import main.scala.transform.DoublesOnlyTransformer
import main.scala.transform.HDayVolatilityTransformer
import main.scala.transform.ValueDateTransformer

class HDayMCSValuePredictorTest extends SparkTestBase {

  var instance: HDayMCSValuePredictor = _

  private var hDayValue: String = _
  private var mcsNumIterations: String = _

  private var modelsLocation: String = _
  private var modelSchemasLocation: String = _

  private var portfolioFileLocation: String = _
  private var portfolioFileType: String = _
  private var keyColumn: String = _
  private var valueColumn: String = _
  private var instrumentColumn: String = _

  private var factorsFileLocation: String = _
  private var factorsFileName: String = _

  override def beforeAll(): Unit = {

    super.beforeAll()
  }

  override def beforeEach() {

    generateContextFileContentValues

    resetTestEnvironment
  }

  // Prevent the Spark Context being recycled
  override def afterEach() {}

  /**
   * Invoking predict with a null portfolio code should result in an exception
   */
  test("predict with a null portfolio code") {

    intercept[IllegalArgumentException] {
      instance.predict(null, null)
    }
  }

  /**
   * Invoking predict with an empty portfolio code should result in an exception
   */
  test("predict with an empty portfolio code") {

    intercept[IllegalArgumentException] {
      instance.predict("", null)
    }
  }

  /**
   * Invoking predict with a null at-date should result in an exception
   */
  test("predict with a null at-date code") {

    intercept[IllegalArgumentException] {
      instance.predict("Portfolio code", null)
    }
  }

  /**
   * Predict using a test portfolio code
   */
  test("predict using a test portfolio code") {

    val expectedPCode = "Test_Portfolio_1"
    val expectedAtDate = LocalDate.of(2016, 6, 1)
    val result = instance.predict(expectedPCode, expectedAtDate)
    result.foreach(t => println(s"${t._2.mkString(", ")}"))
  }

  //
  //
  //
  private def generateContextFileContentValues = {

    hDayValue = "\"10\""
    mcsNumIterations = "\"100\""

    modelsLocation = "\"/project/test/initial-testing/h-models/models/\""
    modelSchemasLocation = "\"/project/test/initial-testing/h-models/schemas/\""

    portfolioFileLocation = "\"/project/test/initial-testing/portfolios/\""
    portfolioFileType = "\".csv\""
    keyColumn = "\"valueDate\""
    valueColumn = "\"value\""
    instrumentColumn = "\"dsCode\""

    factorsFileLocation = "\"/project/test/initial-testing//\""
    factorsFileName = "\"factors.clean.csv\""

  }

  private def generateContextFileContents: String = {

    val hDayVolatilityTransformerConfig = s"hDayVolatility{hDayValue = ${hDayValue}}"
    val mcsConfig = s"mcs{mcsNumIterations = ${mcsNumIterations}}"
    val modelConfig = s"instrumentModel{ modelsLocation = ${modelsLocation} , modelSchemasLocation = ${modelSchemasLocation}}"
    val portfolioConfig = s"""portfolioHolding{fileLocation = ${portfolioFileLocation}
                      , portfolioFileType = ${portfolioFileType} , keyColumn = ${keyColumn}
                      , valueColumn = ${valueColumn}, instrumentColumn = ${instrumentColumn}}"""
    val factorsConfig = s"riskFactor{fileLocation = ${factorsFileLocation}, factorsFileName = ${factorsFileName} }"

    s"""${hadoopAppContextEntry}, ${mcsConfig}, ${hDayVolatilityTransformerConfig}, ${mcsConfig}  
          , ${modelConfig} , ${portfolioConfig} , ${factorsConfig}"""
  }

  private def generateDefaultInstance = {

    // Takes the place of a DI instance
    val p = new PortfolioValuesSourceFromFile()
    val r = new RiskFactorSourceFromFile()
    r.add(new ValueDateTransformer())
    val c = new CholeskyCorrelatedSampleGenerator(new RandomDoubleSourceFromRandom(new ISAACRandom))
    val m = new InstrumentModelSourceFromFile()
    instance = new HDayMCSValuePredictor(p, r, c, m)
  }

  private def generateAppContext {

    val configFile = writeTempFile(generateContextFileContents)
    try {
      val result = ApplicationContext.useConfigFile(configFile)
    } finally {
      configFile.delete()
    }
  }

  private def resetTestEnvironment = {

    ApplicationContext.sc(sc)
    generateContextFileContents
    generateAppContext
    generateDefaultInstance
  }
}