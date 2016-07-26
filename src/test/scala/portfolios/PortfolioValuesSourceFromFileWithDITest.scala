package test.scala.portfolios

import test.scala.application.DITestBase
import main.scala.portfolios.PortfolioValuesSourceFromFile
import java.time.LocalDate

class PortfolioValuesSourceFromFileWithDITest extends DITestBase {

  val instanceBeanName = "defaultPortfolioValuesSourceFromFile"

  var instance: PortfolioValuesSourceFromFile = _

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
  //  Helper functions
  //
  private def generateInstance = {

    super.generateApplicationContext
    instance = ctx.getBean(instanceBeanName).asInstanceOf[PortfolioValuesSourceFromFile]
  }

}