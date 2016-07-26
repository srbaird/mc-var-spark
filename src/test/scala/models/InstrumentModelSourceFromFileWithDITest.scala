package test.scala.models

import main.scala.models.InstrumentModelSourceFromFile
import test.scala.application.DITestBase
import org.apache.spark.ml.tuning.CrossValidatorModel
import main.scala.application.ApplicationContext

class InstrumentModelSourceFromFileWithDITest extends DITestBase {

  val instanceBeanName = "defaultInstrumentModelSourceFromFile"

  var instance: InstrumentModelSourceFromFile = _

  private var testModel: CrossValidatorModel = _

  override def beforeAll(): Unit = {

    super.beforeAll
  }

  override def beforeEach() {

    generateInstance
    testModel = generateCrossValidatorModelFromKnownTestLocation
  }

  // Overridden to prevent Spark Context from being recycled
  override def afterEach() {}

  //
  //  Start of tests
  //
  /**
   * Get a list of the data sets codes
   */
  test("list the available instrument model data set codes") {

    val result = instance.getAvailableModels
    //    assert(!result.isEmpty)
  }

  /**
   * Attempting to read a model using a null dataset code will return an exception
   */
  test("get a model using a null dataset code") {

    intercept[IllegalArgumentException] {
      instance.getModel(null)
    }
  }

  /**
   * Attempting to read a model using an empty dataset code will return an exception
   */
  test("get a model using an empty dataset code") {

    intercept[IllegalArgumentException] {
      instance.getModel("")
    }
  }

  /**
   * Get a model for an non-existent dataset code
   */
  test("read a model for a dataset code that does not exist") {

    val missingDSCode = "UNKNOWN_DSCODE"
    //   assert(!instance.getAvailableModels.contains(missingDSCode))

    //   val result = instance.getModel(missingDSCode)
    //   assert(result == None)
  }

  /**
   * Get a model for a known dataset code
   */
  test("read a model for a known dataset code ") {

    val expectedDSCode = "TEST_DSNAME"
    assert(instance.getAvailableModels.contains(expectedDSCode))

    val result = instance.getModel(expectedDSCode)
    assert(result != None)
  }

  /**
   * Attempting to write a model with a null dataset code  will return an exception
   */
  test("put a model with a null dataset code") {

    intercept[IllegalArgumentException] {
      instance.putModel(null, null)
    }
  }
  /**
   * Attempting to write a model with an empty dataset code  will return an exception
   */
  test("put a model with an empty dataset code") {

    intercept[IllegalArgumentException] {
      instance.putModel("", null)
    }
  }
  /**
   * Attempting to write a null model will return an exception
   */
  test("put a null model") {

    intercept[IllegalArgumentException] {
      instance.putModel("dsCode", null)
    }
  }

  /**
   * Write a model
   */
  test("create a model and persist it ") {

    val expectedDSCode = "TEST_DSNAME"
    instance.putModel(expectedDSCode, testModel)
    assert(instance.getAvailableModels.contains(expectedDSCode))
  }

  /**
   * Attempting to remove a model with a null dataset code  will return an exception
   */
  test("remove a model with a null dataset code") {

    intercept[IllegalArgumentException] {
      instance.removeModel(null)
    }
  }

  /**
   * Attempting to remove a model with an empty dataset code  will return an exception
   */
  test("remove a model with an empty dataset code") {

    intercept[IllegalArgumentException] {
      instance.removeModel("")
    }
  }

  /**
   * Remove a previously created model
   */
  test("remove an existing model") {

    val expectedDSCode = "TEST_DSNAME_FOR_DELETION"
    instance.putModel(expectedDSCode, testModel)
    assert(instance.getAvailableModels.contains(expectedDSCode))

    instance.removeModel(expectedDSCode)
    assert(!instance.getAvailableModels.contains(expectedDSCode))
  }
  //
  //  Helper functions
  //
  private def generateInstance = {

    super.generateApplicationContext
    instance = ctx.getBean(instanceBeanName).asInstanceOf[InstrumentModelSourceFromFile]
  }
  //
  // Use a previously generated model as a base for put() operations
  //
  private def generateCrossValidatorModelFromKnownTestLocation = {

    val ctx = ApplicationContext.getContext
    val hdsfName = ctx.getString("fs.default.name")
    CrossValidatorModel.load(s"${hdsfName}/project/test/initial-testing/models/WIKI_CMC")
  }

}