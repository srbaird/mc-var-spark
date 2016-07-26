package test.scala.prices

import java.time.LocalDate
import main.scala.prices.InstrumentPriceSourceFromFile
import test.scala.application.DITestBase
import main.scala.application.ApplicationContext

class InstrumentPriceSourceFromFileWithDITest extends DITestBase {

  val instanceBeanName = "defaultInstrumentPriceSourceFromFile"

  var instance: InstrumentPriceSourceFromFile = _

  override def beforeAll(): Unit = {

    super.beforeAll
  }

  override def beforeEach() {

    generateInstance
  }

  // Overridden to prevent Spark Context from being recycled
  override def afterEach() {}


  /**
   * Get a list of the datasets contained in the populated test directory.
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
      instance.getPrices(null, LocalDate.of(2016, 5, 1))
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
   * Passing a from-date greater than the to-date argument should result in an exception
   */
  test("test reading by date where from-date exceeds to-date") {

    val expectedDSCode = "TEST_DSNAME"
    intercept[IllegalArgumentException] {
      instance.getPrices(expectedDSCode, LocalDate.of(2016, 5, 2), LocalDate.of(2016, 5, 1))
    }
  }

  /**
   * Get a subset of the test dataset based on a date range
   */
  test("Get a test subset between 01 and 05-May-2016") {

    val expectedDSCode = "TEST_DSNAME"
    val fromDate = LocalDate.of(2016, 5, 1)
    val toDate = LocalDate.of(2016, 5, 5)

    val expectedRowCount = 4L
    val result = instance.getPrices(expectedDSCode, fromDate, toDate)
    assert(result.count() == expectedRowCount)
  }

  /**
   * Get a subset of the test dataset based on a from-date
   */
  test("Get a test subset after 15-May-2016") {

    val expectedDSCode = "TEST_DSNAME"
    val fromDate = LocalDate.of(2016, 5, 15)

    val expectedRowCount = 11L
    val result = instance.getPrices(expectedDSCode, fromDate)

    assert(result.count() == expectedRowCount)
  }

  //
  //
  //
  private def generateInstance = {

    super.generateApplicationContext
    instance = ctx.getBean(instanceBeanName).asInstanceOf[InstrumentPriceSourceFromFile]
  }

}