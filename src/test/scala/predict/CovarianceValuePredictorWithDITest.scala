package test.scala.predict

import main.scala.predict.CovarianceValuePredictor
import test.scala.application.DITestBase
import main.scala.predict.HDayMCSValuePredictor

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
  
    //
  //  Helper functions
  //  ****************
  private def generateInstance = {

    super.generateApplicationContext
    instance = ctx.getBean(instanceBeanName).asInstanceOf[CovarianceValuePredictor]
  }
  
}