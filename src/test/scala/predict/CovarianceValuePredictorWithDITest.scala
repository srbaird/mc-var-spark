package test.scala.predict

import main.scala.predict.CovarianceValuePredictor
import test.scala.application.DITestBase
import main.scala.predict.HDayMCSValuePredictor
import org.apache.commons.math3.stat.correlation.Covariance
import org.apache.commons.math3.linear.Array2DRowRealMatrix
import java.time.LocalDate

class CovarianceValuePredictorWithDITest extends DITestBase {

  val instanceBeanName = "covarianceValuePredictor"

  var instance: CovarianceValuePredictor = _

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
      instance.value(null, null)
    }
  }

  /**
   * Invoking predict with an empty portfolio code should result in an exception
   */
  test("predict with an empty portfolio code") {

    intercept[IllegalArgumentException] {
      instance.value("", null)
    }
  }

  /**
   * Invoking predict with a null at-date should result in an exception
   */
  test("predict with a null at-date code") {

    intercept[IllegalArgumentException] {
      instance.value("Portfolio code", null)
    }
  }

  /**
   * Test the logic
   */
  test("test the covariance matrix") {

    val f = Array(Array(0.01, 0.002), Array(0.002, 0.005))

    val c = new Array2DRowRealMatrix(f)

    // Vector is a nx1 matrix
    val weights = Array(Array(1D), Array(2D))
    val v = new Array2DRowRealMatrix(weights)

    // Transpose to a 1xn matrix
    val vT = v.transpose

    // result should be a 1x1 matrix
    val r = vT.multiply(c.multiply(v))
    assert(r.getColumnDimension == 1)
    assert(r.getRowDimension == 1)
    assert(r.getColumn(0)(0) == 0.038)

  }

  /**
   * Predict using a test portfolio code
   */
  test("predict using a test portfolio code") {

    val expectedPCode = "Test_Portfolio_1"
    val expectedAtDate = LocalDate.of(2016, 6, 2)
    val result = instance.value(expectedPCode, expectedAtDate)
    assert(result.length == 1)  
    println(s"10-day 95% Var = ${1.645 * result(0)._1}")
    println(s"10-day 99% Var = ${2.58 * result(0)._1}")
  }

  //
  //  Helper functions
  //  ****************
  private def generateInstance = {

    super.generateApplicationContext
    instance = ctx.getBean(instanceBeanName).asInstanceOf[CovarianceValuePredictor]
  }

}