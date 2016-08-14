package test.scala.models

import main.scala.models.MLReadableStringBeanFactory
import test.scala.application.DITestBase
import main.scala.models.MLReadableStringBeanFactory
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.ml.tuning.CrossValidatorModel
import main.scala.application.ApplicationContext

class MLReadableStringBeanFactoryWithDITest extends DITestBase {

  val instanceBeanName = "defaultMLReadableStringBeanFactory"

  var instance: MLReadableStringBeanFactory = _

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
  //  attempting to read with a null key should result in an exception
  //  
  test("test read with null key") {

    intercept[IllegalArgumentException] {
      instance.create(null)
    }
  }
  //
  //  Reading a non-existing key should result in an exception
  //  
  test("test read from empty factory") {
    instance = new MLReadableStringBeanFactory()
    intercept[IllegalStateException] {
      instance.create("Any string")
    }
  }

  //
  //  test creating a valid factory
  //  
  test("test create a valid factory") {

    val expectedBeanName = "CrossValidatorModel"
    val result = instance.create(expectedBeanName)
    assert(result.isInstanceOf[MLReadable[_]])

  }
  //
  //  Use an known existing model to load from a factory created instance
  //  
  test("test loading from a valid created factory") {

    val expectedBeanName = "LinearRegressionModel"
    val result = instance.create(expectedBeanName)
    assert(result.isInstanceOf[MLReadable[_]])
    val hdsfName = ApplicationContext.getContext.getString("fs.default.name")
    result.load(s"${hdsfName}/project/test/initial-testing/h-models/models/WIKI_CMC")
  }
  //
  //  Helper functions
  //
  private def generateInstance = {

    super.generateApplicationContext
    instance = ctx.getBean(instanceBeanName).asInstanceOf[MLReadableStringBeanFactory]
  }
}