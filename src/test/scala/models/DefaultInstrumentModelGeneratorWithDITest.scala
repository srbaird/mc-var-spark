package test.scala.models

import org.apache.spark.ml.tuning.CrossValidatorModel
import main.scala.models.DefaultInstrumentModelGenerator
import test.scala.application.DITestBase
import main.scala.application.ApplicationContext
import java.io.File
import java.time.LocalDate

class DefaultInstrumentModelGeneratorWithDITest extends DITestBase {

  val instanceBeanName = "defaultDefaultInstrumentModelGenerator"

  var instance: DefaultInstrumentModelGenerator = _

  val localApplicationContextFileName = "src/test/scala/models/DefaultInstrumentModelGeneratorApplicationContext"

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
   * Passing a from-date greater than the to-date argument should result in an exception
   */
  test("test reading by date where from-date exceeds to-date") {

    intercept[IllegalArgumentException] {
      instance.buildModel(LocalDate.of(2016, 5, 2), LocalDate.of(2016, 5, 1), "Any string")
    }
  }

  /**
   * Default setup should mean that hasSources is true
   */
  test("test the default setup returns true for hasSources") {

    assert(instance.hasSources)
  }

  /**
   * Setup without constructor arguments should mean that hasSources is false
   */
  test("test setup without constructor arguments") {

    instance = new DefaultInstrumentModelGenerator()
    assert(!instance.hasSources)
  }

  
  //
  //  Helper functions
  //  ****************
  private def generateInstance = {

    super.generateApplicationContext
    ApplicationContext.useConfigFile(new File(localApplicationContextFileName)) // use local version of the application context file
    instance = ctx.getBean(instanceBeanName).asInstanceOf[DefaultInstrumentModelGenerator]
  }
}