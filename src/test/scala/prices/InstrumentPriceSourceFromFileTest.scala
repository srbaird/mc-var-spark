package test.scala.prices

import org.apache.hadoop.fs.FileSystem
import main.scala.application.ApplicationContext
import main.scala.prices.InstrumentPriceSourceFromFile
import test.scala.application.SparkTestBase
import org.apache.hadoop.conf.Configuration
import java.time.LocalDate

class InstrumentPriceSourceFromFileTest extends SparkTestBase {

  var instance: InstrumentPriceSourceFromFile = _

  // Test file location. Some of the test conditions are linked to the contents
  val hdfsLocation = "\"hdfs://localhost:54310\""
  val fileLocation = "\"/project/test/initial-testing/prices/\""
  val priceFileType = "\".csv\""

  override def beforeAll(): Unit = {

    super.beforeAll()

  }

  override def beforeEach() {

    // Create a temporary config file to specify the test data to use
    // This is generated before each test case as expected outcomes depend on different configurations
    val configFileContents = s"instrumentPrice{hdfsLocation = ${hdfsLocation}, fileLocation = ${fileLocation}, priceFileType = ${priceFileType} }"
    val configFile = writeTempFile(s"${hadoopAppContextEntry}, ${configFileContents}") // Prepend the Hadoop dependencies
    try {
      val result = ApplicationContext.useConfigFile(configFile)
    } finally {
      configFile.delete()
    }
    instance = new InstrumentPriceSourceFromFile(sc) // Needs the Application context to be available

  }
  /**
   * Overridden to prevent Spark Context from being recycled
   */
  override def afterEach = {}

  /**
   * Get a list of the datasets contained in the non-populated test directory
   */
  test("list the available price data sets from an empty directory") {

    val emptyDirectory = "intentionally-empty-for-testing--do-NOT-populate"
    // Generate a new Application context using a modified  file to point the source to an empty directory
    val configFileContents = s"instrumentPrice{hdfsLocation = ${hdfsLocation}, fileLocation = ${fileLocation}/${emptyDirectory}/, priceFileType = ${priceFileType} }"
    val configFile = writeTempFile(s"${hadoopAppContextEntry}, ${configFileContents}")
    try {
      val result = ApplicationContext.useConfigFile(configFile)
    } finally {
      configFile.delete()
    }
    instance = new InstrumentPriceSourceFromFile(sc) // Recreate the instance to pick up new Application Context
    val result = instance.getAvailableCodes()
    assert(result.isEmpty)

  }
  /**
   * Get a list of the datasets contained in the non-populated test directory.
   */
  test("list the available price data sets from a populated directory") {

    val expectedDSCode = "TEST_DSNAME"
    val result = instance.getAvailableCodes()
    assert(!result.isEmpty)
    assert(result.contains(expectedDSCode))

  }

  /**
   * Passing a null dataset code argument should result in an exception
   */
  test("test  getting prices with a null dataset code") {

    intercept[IllegalArgumentException] {
      instance.getPrices(null)
    }
  }

  /**
   * Passing an empty dataset code argument should result in an exception
   */
  test("test  getting prices with an empty dataset code") {

    intercept[IllegalArgumentException] {
      instance.getPrices("")
    }
  }

  /**
   * Get a full set of prices for the test dataset code
   */
  test("Get the full test dataset") {

    val expectedDSCode = "TEST_DSNAME"
    val expectedRowCount = 21L
    val result = instance.getPrices(expectedDSCode)
    assert(result.count() == expectedRowCount)
  }

  /**
   * Passing an empty dataset code argument should result in an exception
   */
  test("test reading by date with an empty dataset code argument") {

    intercept[IllegalArgumentException] {
      instance.getPrices("", LocalDate.of(2106, 5, 1))
    }
  }
  
    /**
   * Passing a null dataset code argument should result in an exception
   */
  test("test reading by date with a null dataset code argument") {

    intercept[IllegalArgumentException] {
      instance.getPrices(null, LocalDate.of(2106, 5, 1))
    }
  }

  /**
   * Passing a null start date argument should result in an exception
   */
  test("test reading by date with a null start date") {

    val expectedDSCode = "TEST_DSNAME"
    intercept[IllegalArgumentException] {
      instance.getPrices(expectedDSCode, null)
    }
  }

  /**
   * Get a subset of the test dataset based on a date range
   */
  test("Get a test subset between 01 and 05-May-2016") {

    val expectedDSCode = "TEST_DSNAME"
    val fromDate = LocalDate.of(2106, 5, 1)
    val toDate = LocalDate.of(2016, 5, 5)

    val expectedRowCount = 4L
    //val result = instance.getPrices(expectedDSCode,fromDate, toDate)
    // assert(result.count() == expectedRowCount)
  }
}