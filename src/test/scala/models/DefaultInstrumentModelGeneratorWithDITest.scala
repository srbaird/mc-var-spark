package test.scala.models

import org.apache.spark.ml.tuning.CrossValidatorModel
import main.scala.models.DefaultInstrumentModelGenerator
import test.scala.application.DITestBase
import main.scala.application.ApplicationContext
import java.io.File

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
   *
   */
  test("test building the instance from the context file") {

    println("Built")
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