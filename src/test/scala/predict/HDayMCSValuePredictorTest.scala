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

class HDayMCSValuePredictorTest extends SparkTestBase {

  var instance: HDayMCSValuePredictor = _

  private var hDayValue: String = _
  private var mcsNumIterations: String = _

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
    val expectedAtDate = LocalDate.of(2016, 5, 1)
    instance.predict(expectedPCode, expectedAtDate)
  }

  //
  //
  //
  private def generateContextFileContentValues = {

    hDayValue = "\"10\""
    mcsNumIterations = "\"10000\""
  }

  private def generateContextFileContents: String = {

    val hDayVolatilityTransformerConfig = s"hDayVolatility{hDayValue = ${hDayValue}}"
    val mcsConfig = s"mcs{mcsNumIterations = ${mcsNumIterations}}"
    s"${hadoopAppContextEntry}, ${mcsConfig}, ${hDayVolatilityTransformerConfig}" // Prepend the Hadoop dependencies

  }

  private def generateDefaultInstance = {

    // Takes the place of a DI instance
    val p = new PortfolioValuesSourceFromFile()
    val r =  new RiskFactorSourceFromFile()
    val c = new CholeskyCorrelatedSampleGenerator(new RandomDoubleSourceFromRandom(new ISAACRandom))
    val m = new InstrumentModelSourceFromFile()
    instance = new HDayMCSValuePredictor(p, r, c, m )
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