package test.scala.predict

import main.scala.predict.CholeskyCorrelatedSampleGenerator
import test.scala.application.DITestBase
import main.scala.predict.RandomDoubleSource

class RandomDistributionTest extends DITestBase {

  val instanceBeanName = "inverseRandomDoubleSource"

  var instance: RandomDoubleSource = _

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
  test("Create instance test") {

    assert(instance != null)
  }

  /**
   * Generate two sequences of random numbers and ensure they are different. The instance is recreated 
   * to test that distributed instantiations do not start their sequences from the same point
   */
  test("test independent random sequences are not equal") {

    val matrix1 = instance.randomMatrix(1, 10)
    generateInstance
    val matrix2 = instance.randomMatrix(1, 10)
    
    (0 to matrix1(0).length - 1).foreach( i => assert(matrix1(0)(i) != matrix2(0)(i)))
    
  }

  //
  //  Helper functions
  //  ****************
  private def generateInstance = {

    super.generateApplicationContext
    instance = ctx.getBean(instanceBeanName).asInstanceOf[RandomDoubleSource]
  }

}