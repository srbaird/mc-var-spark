package test.scala.application

import main.scala.application.ApplicationContext
import collection.JavaConversions._

class ConfigTest extends DITestBase {

  override def beforeAll(): Unit = {

    super.beforeAll
  }

  override def beforeEach() {

    generateInstance
  }

  // Overridden to prevent Spark Context from being recycled
  override def afterEach() {}

  test("Read percentiles list") {

    val expectedListName = "valueAtRisk.writePercentilesList"

    val percentilesList = ApplicationContext.getContext.getLongList(expectedListName).toList
    
    val expectedRange = (95L to 99L)
    assert(expectedRange.forall { x => percentilesList.contains(x)})  
    
  }
  //
  //
  //
  private def generateInstance = {

    super.generateApplicationContext
  }
}