package test.scala.models

import main.scala.factors.RiskFactorSourceFromFile
import test.scala.application.DITestBase
import java.io.File
import main.scala.application.ApplicationContext
import main.scala.models.DefaultInstrumentModelGenerator
import main.scala.prices.InstrumentPriceSourceFromFile
import main.scala.models.InstrumentModelSourceFromFile

class LongRunDefaultInstrumentModelGeneratorWithDITest extends DITestBase {

  val instanceBeanName = "defaultDefaultInstrumentModelGenerator"

  var instance: DefaultInstrumentModelGenerator = _

  val localApplicationContextFileName = "src/test/scala/models/DefaultInstrumentModelGeneratorApplicationContext"

  override def beforeAll(): Unit = {

    super.beforeAll
  }

  override def beforeEach() {

    generateInstance

  }

  // Overridden to prevent Spark Context from being recycled
  override def afterEach() {}

  //
  //  Start of tests
  //  **************
  /**
   * Generating a model with a null dataset code should result in an exception
   */
  test("test generating model with null dataset code") {

    val nullDatasetCode: String = null
    intercept[IllegalArgumentException] {
      instance.buildModel(nullDatasetCode)
    }
  }

  /**
   * Generating a model with a null dataset code array should result in an exception
   */
  test("test generating model with null dataset code array") {

    val nullDatasetCode: Array[String] = null
    intercept[IllegalArgumentException] {
      instance.buildModel(nullDatasetCode)
    }
  }

  /**
   * Generating a model with an empty dataset code should result in an exception
   */
  test("test generating model with an empty dataset code") {

    intercept[IllegalArgumentException] {
      instance.buildModel("")
    }
  }

  /**
   * Generating a model without required dependencies should result in an exception
   */
  test("test generating model without setting dependencies") {

    instance = new DefaultInstrumentModelGenerator()
    intercept[IllegalStateException] {
      instance.buildModel("AnyString")
    }
  }



  /**
   * Generating a model with no dataset code prices
   */
  test("test generating model without dataset price data") {

    val expectedDSCode = "AnyString"
    val result = instance.buildModel(expectedDSCode)
    assert(!result(expectedDSCode)._1)

  }

  /**
   * Generating a model with existing code prices
   */
  test("test generating model with existing dataset price data") {

    val availableCodes = new InstrumentPriceSourceFromFile().getAvailableCodes()

    val expectedDSCode = "TEST_DSNAME_FULL" // 
    assert(availableCodes.contains(expectedDSCode))

    // Remove the model
    val instrumentModelSource = new InstrumentModelSourceFromFile()
    instrumentModelSource.removeModel(expectedDSCode)
    assert(!instrumentModelSource.getAvailableModels.contains(expectedDSCode))

    val result = instance.buildModel(expectedDSCode)
    println(s"Received: ${result(expectedDSCode)._2}")
    assert(result(expectedDSCode)._1)

    assert(instrumentModelSource.getAvailableModels.contains(expectedDSCode))
  }

  /**
   * Generating a model with existing code prices
   */
  test("test generating two models with existing dataset price data") {

    val availableCodes = new InstrumentPriceSourceFromFile().getAvailableCodes()

    val expectedDSCode1 = "TEST_DSNAME_FULL" // 
    val expectedDSCode2 = "TEST_DSNAME_FULL2" // 
    assert(availableCodes.contains(expectedDSCode1))
    assert(availableCodes.contains(expectedDSCode2))

    // Remove the models
    val instrumentModelSource = new InstrumentModelSourceFromFile()
    instrumentModelSource.removeModel(expectedDSCode1)
    instrumentModelSource.removeModel(expectedDSCode2)
    assert(!instrumentModelSource.getAvailableModels.contains(expectedDSCode1))
    assert(!instrumentModelSource.getAvailableModels.contains(expectedDSCode2))

    val result = instance.buildModel(expectedDSCode1, expectedDSCode2)

    println(s"Received: ${result(expectedDSCode1)._2}")
    assert(result(expectedDSCode1)._1)
    println(s"Received: ${result(expectedDSCode2)._2}")
    assert(result(expectedDSCode2)._1)

    assert(instrumentModelSource.getAvailableModels.contains(expectedDSCode1))
    assert(instrumentModelSource.getAvailableModels.contains(expectedDSCode2))
  }
  //
  //  Helper functions
  //  ****************
  private def generateInstance = {

    super.generateApplicationContext
    ApplicationContext.useConfigFile(new File(localApplicationContextFileName)) // use local version of the application context file
    instance = ctx.getBean(instanceBeanName).asInstanceOf[DefaultInstrumentModelGenerator]
  }
}