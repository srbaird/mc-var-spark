package test.scala.factors

import main.scala.factors.RiskFactorSourceFromFile
import test.scala.application.DITestBase
import java.time.LocalDate
import main.scala.application.ApplicationContext
import java.io.File

class RiskFactorSourceFromFileWithDITest extends DITestBase {

  val instanceBeanName = "defaultRiskFactorSourceFromFile"

  var instance: RiskFactorSourceFromFile = _

  val localApplicationContextFileName = "src/test/scala/factors/RiskFactorSourceFromFIleApplicationContext"

  // Some of the test conditions are linked to the test file contents
  val testFileLength = 31L

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
  //
  /**
   *
   */
  test("test reading a negative number of rows") {

    intercept[IllegalArgumentException] {
      instance.head(-1)
    }
  }

  /**
   *
   */
  test("test reading zero rows") {

    intercept[IllegalArgumentException] {
      instance.head(0)
    }
  }
  /**
   * The test file is assumed to have at least one row in it. The assertion of content is dependent on the file
   */
  test("test reading a single row") {

    val result = instance.head(1)
    assert(result.count() == 1)
  }
  /**
   * The test file is assumed to have fewer than 100 rows
   */
  test("test reading 100 rows from smaller data set") {

    val result = instance.head(100)
    assert(result.count() == testFileLength) // Test file has only 31 rows
  }

  /**
   * Passing a null start date argument should result in an exception
   */
  test("test reading by date with a null start date") {

    intercept[IllegalArgumentException] {
      instance.factors(null)
    }
  }

  /**
   * Return all rows in the file
   */
  test("test reading all rows ") {
    val result = instance.factors()
    assert(result.count() == testFileLength) // Test file has only 31 rows
  }

  /**
   * Passing a from-date that is greater than the to-date should result in an exception
   */
  test("test reading by date where from-date is greater than to-date") {

    val day = 1
    val month = 1
    val year = 2016

    val toDate = LocalDate.of(year, month, day)
    val fromDate = LocalDate.of(year, month, day + 1)
    intercept[IllegalArgumentException] {
      instance.factors(fromDate, toDate)
    }
  }

  /**
   * The test file is assumed to contain all rows after 01-Jan-2016
   */
  test("test reading rows with a start date >= 01-Jan-2016") {

    val day = 1
    val month = 1
    val year = 2016
    val fromDate = LocalDate.of(year, month, day)
    val result = instance.factors(fromDate)
    assert(result.count() == testFileLength) // All 
  }

  /**
   * The test file is assumed to contain some rows after 15-May-2016
   */
  test("test reading rows with a start date >= 15-May-2016") {

    val expectedNumRows = 17L
    val day = 15
    val month = 5
    val year = 2016
    val fromDate = LocalDate.of(year, month, day)
    val result = instance.factors(fromDate)
    assert(result.count() == expectedNumRows)
  }

  /**
   * The test file is assumed to contain some rows between 01 and 02 May 2016
   */
  test("test reading rows with a between 01-May-2016 and 02-May-2016") {

    val expectedNumRows = 2L
    val day = 1
    val month = 5
    val year = 2016
    val fromDate = LocalDate.of(year, month, day)
    val toDate = LocalDate.of(year, month, day + 1)
    val result = instance.factors(fromDate, toDate)
    assert(result.count() == expectedNumRows)
  }

  //
  //  Helper functions
  //
  private def generateInstance = {

    super.generateApplicationContext
    ApplicationContext.useConfigFile(new File(localApplicationContextFileName)) // use local version of the application context file
    instance = ctx.getBean(instanceBeanName).asInstanceOf[RiskFactorSourceFromFile]
  }

}