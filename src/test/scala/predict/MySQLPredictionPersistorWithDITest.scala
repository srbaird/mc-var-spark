package test.scala.predict

import main.scala.predict.MySQLPredictionPersistor
import test.scala.application.DITestBase
import java.time.LocalDate

class MySQLPredictionPersistorWithDITest extends DITestBase {

  val instanceBeanName = "mySQLPredictionPersistor"

  val expectedPortfolioCode = "portfolio code"
  val expectedAtDate = LocalDate.now()
  val expectedEstimatorClass = "Test only"
  val expectedHValue = 10
  val expectedPValue = 95
  val expectedValuation = System.currentTimeMillis()

  var instance: MySQLPredictionPersistor = _

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
  //  **************
  /**
   * Writing a row with a null portfolio code should result in an exception
   */
  test("write row with a null portfolio code") {

    intercept[IllegalArgumentException] {
      instance.persist(null, expectedAtDate, expectedEstimatorClass, expectedHValue, expectedPValue, expectedValuation)
    }
  }

  /**
   * Writing a row with an empty portfolio code should result in an exception
   */
  test("write row with an empty portfolio code") {

    intercept[IllegalArgumentException] {
      instance.persist("", expectedAtDate, expectedEstimatorClass, expectedHValue, expectedPValue, expectedValuation)
    }
  }
  
  /**
   * Writing a row with a null at-date should result in an exception
   */
  test("write row with a null at-date") {

    intercept[IllegalArgumentException] {
      instance.persist(expectedPortfolioCode, null, expectedEstimatorClass, expectedHValue, expectedPValue, expectedValuation)
    }
  }

  /**
   * Writing a row with a null estimator class should result in an exception
   */
  test("write row with a null estimator class") {

    intercept[IllegalArgumentException] {
      instance.persist(expectedPortfolioCode, expectedAtDate, null, expectedHValue, expectedPValue, expectedValuation)
    }
  }

  /**
   * Writing a row with an empty estimator class should result in an exception
   */
  test("write row with an empty estimator class") {

    intercept[IllegalArgumentException] {
      instance.persist(expectedPortfolioCode, expectedAtDate, "", expectedHValue, expectedPValue, expectedValuation)
    }
  }

  /**
   * Write a row to the table
   */
  test("write a prediction row ") {

    val expectedPortfolioCode = "portfolio code"
    val expectedAtDate = LocalDate.now()
    val expectedEstimatorClass = "Test only"
    val expectedHValue = 10
    val expectedPValue = 95
    val expectedValuation = System.currentTimeMillis()
    instance.persist(expectedPortfolioCode, expectedAtDate, expectedEstimatorClass, expectedHValue, expectedPValue, expectedValuation)
  }
  //
  //  Helper functions
  //  ****************
  private def generateInstance = {

    super.generateApplicationContext
    instance = ctx.getBean(instanceBeanName).asInstanceOf[MySQLPredictionPersistor]
  }
}