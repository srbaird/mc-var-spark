package test.scala.models

import main.scala.models.DefaultInstrumentModelGenerator
import test.scala.application.DITestBase
import java.time.LocalDate
import main.scala.application.ApplicationContext

class HDayLongRunModelGeneratorWithDITest extends DITestBase {

  val instanceBeanName = "hDayInstrumentModelGenerator"

  var instance: DefaultInstrumentModelGenerator = _

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
   * Generate a model from the full set of factors/prices over a 250 day period
   */
  test("test generating a valid dataset code between 01-Jun-2015 and 31-May-2016") {

    val expectedDSCode = "WIKI_CMC"
    val result = instance.buildModel(LocalDate.of(2015, 6, 1), LocalDate.of(2016, 5, 31), expectedDSCode)
    println(s"Test returned '${result(expectedDSCode)._2}' for ${expectedDSCode} ")
    assert(result(expectedDSCode)._1)
  }
  //
  //  Helper functions
  //  ****************
  private def generateInstance = {

    super.generateApplicationContext
    instance = ctx.getBean(instanceBeanName).asInstanceOf[DefaultInstrumentModelGenerator]
  }
}