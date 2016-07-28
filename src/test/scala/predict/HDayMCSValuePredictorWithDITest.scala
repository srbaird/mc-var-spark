package test.scala.predict

import test.scala.application.DITestBase
import main.scala.predict.HDayMCSValuePredictor
import java.time.LocalDate
import main.scala.application.ApplicationContext

class HDayMCSValuePredictorWithDITest extends DITestBase {

  val instanceBeanName = "hDayMCSValuePredictor"

  var instance: HDayMCSValuePredictor = _

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
  test("test construction of instance from DI") {
    println(s"${instance}")
  }

  /**
   * Invoking predict with a null portfolio code should result in an exception
   */
  test("predict with a null portfolio code") {

    intercept[IllegalArgumentException] {
      instance.predict(null, null)
    }
  }

  /**
   * Invoking predict with an empty portfolio code should result in an exception
   */
  test("predict with an empty portfolio code") {

    intercept[IllegalArgumentException] {
      instance.predict("", null)
    }
  }

  /**
   * Invoking predict with a null at-date should result in an exception
   */
  test("predict with a null at-date code") {

    intercept[IllegalArgumentException] {
      instance.predict("Portfolio code", null)
    }
  }

  /**
   * Predict using a test portfolio code
   */
  test("predict using a test portfolio code") {

    val expectedPCode = "Test_Portfolio_1"
    val expectedAtDate = LocalDate.of(2016, 6, 1)
    val result = instance.predict(expectedPCode, expectedAtDate)
    val mcsNumIterationsInt = ApplicationContext.getContext.getLong("mcs.mcsNumIterations")
    assert(result.length == mcsNumIterationsInt)
  }

  //
  //  Helper functions
  //  ****************
  private def generateInstance = {

    super.generateApplicationContext
    instance = ctx.getBean(instanceBeanName).asInstanceOf[HDayMCSValuePredictor]
  }
}