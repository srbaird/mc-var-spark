package test.scala.predict

import test.scala.application.DITestBase
import main.scala.predict.ObservationValueGenerator
import java.time.LocalDate

class ObservationValueGeneratorWithDITest extends DITestBase {

  val instanceBeanName = "observationValueGenerator"

  var instance: ObservationValueGenerator = _

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
   * Invoking value with a null portfolio code should result in an exception
   */
  test("predict with a null portfolio code") {

    intercept[IllegalArgumentException] {
      instance.value(null, null)
    }
  }

  /**
   * Invoking value with an empty portfolio code should result in an exception
   */
  test("predict with an empty portfolio code") {

    intercept[IllegalArgumentException] {
      instance.value("", null)
    }
  }

  /**
   * Invoking value with a null at-date should result in an exception
   */
  test("predict with a null at-date code") {

    intercept[IllegalArgumentException] {
      instance.value("Portfolio code", null)
    }
  }
    /**
   * Value using a test portfolio code
   */
  test("value using a test portfolio code") {

    val expectedPCode = "Test_Portfolio_1"
    val expectedAtDate = LocalDate.of(2016, 6, 2)
    val result = instance.value(expectedPCode, expectedAtDate)
    assert(result.length == 1)
    
    // TODO: check values
    // 
    result.foreach(f => println(s"Value = ${f._1}: ${f._2.mkString(", ")}"))

  
  }

  
  //
  //  Helper functions
  //  ****************
  private def generateInstance = {

    super.generateApplicationContext
    instance = ctx.getBean(instanceBeanName).asInstanceOf[ObservationValueGenerator]
  }
}