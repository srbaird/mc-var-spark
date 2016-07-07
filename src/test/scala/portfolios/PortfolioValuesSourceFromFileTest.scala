package test.scala.portfolios

import java.time.LocalDate

import main.scala.application.ApplicationContext
import main.scala.prices.PortfolioValuesSourceFromFile
import main.scala.transform.ValueDateTransformer
import test.scala.application.SparkTestBase

class PortfolioValuesSourceFromFileTest extends SparkTestBase {

  var instance: PortfolioValuesSourceFromFile = _
  //
  var fileLocation: String = _
  var portfolioFileType: String = _
  var keyColumn: String = _
  var valueColumn: String = _

  override def beforeAll(): Unit = {

    super.beforeAll()
  }

  override def beforeEach() {

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
      instance.getHoldings(null, LocalDate.of(2106, 5, 1))
    }
  }

  /**
   * Passing an empty portfolio code argument should result in an exception
   */
  test("test getting holdings with an empty portfolio code") {

    intercept[IllegalArgumentException] {
      instance.getHoldings("", LocalDate.of(2106, 5, 1))
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

  //
  // Helper methods to create  valid test environment
  //
  private def generateContextFileContentValues = {

    fileLocation = "\"/project/test/initial-testing/portfolios/\""
    portfolioFileType = "\".csv\""
    keyColumn = "\"valueDate\""
    valueColumn = "\"value\""
  }

  private def generateContextFileContents: String = {

    val portfolioSourceContents = s"""portfolioHolding{fileLocation = ${fileLocation}
                      , portfolioFileType = ${portfolioFileType} , keyColumn = ${keyColumn}, valueColumn = ${valueColumn}}"""

    s"${hadoopAppContextEntry}, ${portfolioSourceContents}" // Prepend the Hadoop dependencies
  }

  private def generateDefaultInstance = {

    instance = new PortfolioValuesSourceFromFile(sc)
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