package test.scala.predict

import test.scala.application.DITestBase
import main.scala.predict.ObservationValueGenerator

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
  
  //
  //  Helper functions
  //  ****************
  private def generateInstance = {

    super.generateApplicationContext
    instance = ctx.getBean(instanceBeanName).asInstanceOf[ObservationValueGenerator]
  }
}