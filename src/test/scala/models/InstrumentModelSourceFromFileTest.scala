package test.scala.models

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import main.scala.application.ApplicationContext
import main.scala.models.InstrumentModelSourceFromFile
import test.scala.application.SparkTestBase

class InstrumentModelSourceFromFileTest extends SparkTestBase {

  var instance: InstrumentModelSourceFromFile = _

  // Test file location. Some of the test conditions are linked to the contents
  val modelLocation = "\"/project/test/initial-testing/models/\""

  override def beforeAll(): Unit = {

    super.beforeAll()

    // Create a temporary config file to specify the test data to use
    val configFileContents = s"instrumentModel{ modelLocation = ${modelLocation} }"
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

}