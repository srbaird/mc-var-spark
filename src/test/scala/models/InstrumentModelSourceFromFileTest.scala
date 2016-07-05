package test.scala.models

import main.scala.application.ApplicationContext
import main.scala.models.InstrumentModelSourceFromFile
import test.scala.application.SparkTestBase

class InstrumentModelSourceFromFileTest extends SparkTestBase {

  var instance: InstrumentModelSourceFromFile = _

  // Test file location. Some of the test conditions are linked to the contents
  val modelsLocation = "\"/project/test/initial-testing/model/models/\""
  val modelSchemasLocation = "\"/project/test/initial-testing/model/schemas/\""

  override def beforeAll(): Unit = {

    super.beforeAll()

    // Create a temporary config file to specify the test data to use
    val configFileContents = s"instrumentModel{ modelsLocation = ${modelsLocation} , modelSchemasLocation = ${modelSchemasLocation}}"
    val configFile = writeTempFile(s"${hadoopAppContextEntry}, ${configFileContents}") // Prepend the Hadoop dependencies

    try {
      val result = ApplicationContext.useConfigFile(configFile)
    } finally {
      configFile.delete()
    }

  }

  override def beforeEach() {

    instance = new InstrumentModelSourceFromFile(sc)
  }

  /**
   * Overridden to prevent Spark Context from being recycled
   */
  override def afterEach = {}

  /**
   * Get a list of the data sets codes
   */
  test("list the available instrument model data set codes") {

    val result = instance.getAvailableModels
    assert(!result.isEmpty)
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

    val missingDSCode = "TEST_DSCODE"
    assert(!instance.getAvailableModels.contains(missingDSCode))

    val result = instance.getModel(missingDSCode)
    assert(result == None)
  }

  /**
   * Get a model for a known dataset code
   */
  test("read a model for a known dataset code ") {

    val missingDSCode = "WIKI_CMC"
    assert(instance.getAvailableModels.contains(missingDSCode))

    val result = instance.getModel(missingDSCode)
    assert(result != None)
  }

}