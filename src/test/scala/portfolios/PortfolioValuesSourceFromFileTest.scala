package test.scala.portfolios

import java.time.LocalDate
import org.apache.spark.annotation.Experimental
import main.scala.application.ApplicationContext
import main.scala.transform.ValueDateTransformer
import test.scala.application.SparkTestBase
import org.apache.spark.sql.types.DataTypes
import main.scala.portfolios.PortfolioValuesSourceFromFile

class PortfolioValuesSourceFromFileTest extends SparkTestBase {

  var instance: PortfolioValuesSourceFromFile = _
  //
  var fileLocation: String = _
  var portfolioFileType: String = _
  var keyColumn: String = _
  var valueColumn: String = _
  var instrumentColumn: String = _

  override def beforeAll(): Unit = {

    super.beforeAll()
  }

  override def beforeEach() {

    ApplicationContext.sc(sc)

    generateContextFileContentValues

    generateContextFileContents

    generateAppContext

    generateDefaultInstance
  }

  // Prevent the Spark Context being recycled
  override def afterEach() {}

  /**
   * Get a list of the portfolio codes contained in the non-populated test directory
   */
  test("list the available portfolio codes from an empty directory") {

    // Reset the application context so that the instance reads from a known empty directory
    fileLocation = "\"/project/test/initial-testing/intentionally-empty-for-testing--do-NOT-populate/\""
    generateContextFileContents
    generateAppContext
    generateDefaultInstance

    val result = instance.getAvailableCodes()
    assert(result.isEmpty)
  }
  /**
   * Get a list of portfolio codes from the correct directory
   */
  test("list the available portfolio codes from a populated directory") {

    val expectedPCode = "Test_Portfolio_1"
    val result = instance.getAvailableCodes()
    assert(!result.isEmpty)
    assert(result.contains(expectedPCode))
  }

  /**
   * Passing a null portfolio code argument should result in an exception
   */
  test("test getting holdings with a null portfolio code") {

    intercept[IllegalArgumentException] {
      instance.getHoldings(null, LocalDate.of(2016, 5, 1))
    }
  }

  /**
   * Passing an empty portfolio code argument should result in an exception
   */
  test("test getting holdings with an empty portfolio code") {

    intercept[IllegalArgumentException] {
      instance.getHoldings("", LocalDate.of(2016, 5, 1))
    }
  }

  /**
   * Passing a null at-date code argument should result in an exception
   */
  test("test getting holdings with a null at-date code") {

    intercept[IllegalArgumentException] {
      instance.getHoldings("AnyString", null)
    }
  }

  /**
   * Passing an unknown portfolio code argument should result in an exception
   */
  test("test getting holdings with an unknown portfolio code") {

    intercept[IllegalStateException] {
      instance.getHoldings("AnyString", LocalDate.of(2016, 5, 1))
    }
  }

  /**
   * Get the holdings at a date in the far past should result in an empty data frame
   */
  test("test getting holdings at a date in the far past") {

    val expectedPCode = "Test_Portfolio_1"
    val expectedDate = LocalDate.of(1900, 1, 1)
    val result = instance.getHoldings(expectedPCode, expectedDate)
    assert(result.count() == 0)
  }

  /**
   * Get the holdings at a date in the far future should result in a data frame with a single row
   * for each instrument
   */
  test("test getting holdings at a date in the far future") {

    val expectedPCode = "Test_Portfolio_1"
    val expectedDate = LocalDate.of(2900, 1, 1)
    val result = instance.getHoldings(expectedPCode, expectedDate)
    assert(result.count() == 1)
  }

  /**
   * Get the holdings at a date in the file
   */
  test("test getting holdings at a date in the file") {

    val expectedPCode = "Test_Portfolio_1"
    val expectedDate = LocalDate.of(2016, 5, 1)
    val result = instance.getHoldings(expectedPCode, expectedDate)
    assert(result.count() == 1)
    //    val v = result.head().getDate(5)
    //    assert(v == expectedDate)
  }

  //
  // Helper methods to create  valid test environment
  //
  private def generateContextFileContentValues = {

    fileLocation = "\"/project/test/initial-testing/portfolios/\""
    portfolioFileType = "\".csv\""
    keyColumn = "\"valueDate\""
    valueColumn = "\"value\""
    instrumentColumn = "\"dsCode\""
  }

  private def generateContextFileContents: String = {

    val portfolioSourceContents = s"""portfolioHolding{fileLocation = ${fileLocation}
                      , portfolioFileType = ${portfolioFileType} , keyColumn = ${keyColumn}
                      , valueColumn = ${valueColumn}, instrumentColumn = ${instrumentColumn}}"""

    s"${hadoopAppContextEntry}, ${portfolioSourceContents}" // Prepend the Hadoop dependencies
  }

  private def generateDefaultInstance = {

    instance = new PortfolioValuesSourceFromFile()
    // TODO: move the dependencies to DI implementation
    instance.add(new ValueDateTransformer())
  }

  private def generateAppContext {

    val configFile = writeTempFile(generateContextFileContents)
    try {
      val result = ApplicationContext.useConfigFile(configFile)
    } finally {
      configFile.delete()
    }
  }
}