package test.scala.portfolios

import main.scala.prices.PortfolioValuesSourceFromFile
import test.scala.application.SparkTestBase
import main.scala.application.ApplicationContext
import main.scala.transform.ValueDateTransformer

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
    fileLocation = "\"/project/test/initial-testing/portfolios/intentionally-empty-for-testing--do-NOT-populate/\""
    beforeEach

    val result = instance.getAvailableCodes()
    assert(result.isEmpty)
  }
  /**
   * Get a list of portfolio codes from the correct directory
   */
  test("list the available portfolio codes from a populated directory") {

    val expectedPCode = "TEST_PNAME"
    val result = instance.getAvailableCodes()
    assert(!result.isEmpty)
    assert(result.contains(expectedPCode))

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