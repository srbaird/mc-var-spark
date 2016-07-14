package test.scala.predict

import main.scala.application.ApplicationContext
import main.scala.predict.HDayMCSValuePredictor
import test.scala.application.SparkTestBase

class HDayMCSValuePredictorTest  extends SparkTestBase {

  var instance: HDayMCSValuePredictor = _
 

  
  private var hDayValue: String = _
  
  override def beforeAll(): Unit = {

    super.beforeAll()

  }


  override def beforeEach() {

    generateContextFileContentValues

    resetTestEnvironment
  }

  // Prevent the Spark Context being recycled
  override def afterEach() {}

  
  
  
  
    //
  //
  //
  private def generateContextFileContentValues = {

    hDayValue = "\"10\""

  }

  private def generateContextFileContents: String = {

    val hDayVolatilityTransformerConfig = s"hDayVolatility{hDayValue = ${hDayValue}}"
    s"${hadoopAppContextEntry}, ${hDayVolatilityTransformerConfig}" // Prepend the Hadoop dependencies

  }

  private def generateDefaultInstance = {

 //   instance = new HDayVolatilityTransformer()
  }

  private def generateAppContext {

    val configFile = writeTempFile(generateContextFileContents)
    try {
      val result = ApplicationContext.useConfigFile(configFile)
    } finally {
      configFile.delete()
    }
  }

  private def resetTestEnvironment = {

    generateContextFileContents
    generateAppContext
    generateDefaultInstance
  }
}