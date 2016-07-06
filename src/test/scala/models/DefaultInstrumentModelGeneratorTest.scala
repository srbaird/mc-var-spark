package test.scala.models

import main.scala.application.ApplicationContext
import main.scala.models.DefaultInstrumentModelGenerator
import test.scala.application.SparkTestBase

class DefaultInstrumentModelGeneratorTest extends SparkTestBase {

  var instance: DefaultInstrumentModelGenerator = _
  //
  val hdfsLocation = "\"hdfs://localhost:54310\""
  val fileLocation = "\"/project/test/initial-testing/prices/\""
  val priceFileType = "\".csv\""
  val keyColumn = "\"valueDate\""
  val valueColumn = "\"closePrice\""

  val instrumentPriceSourceConfig = s"""instrumentPrice{hdfsLocation = ${hdfsLocation}, fileLocation = ${fileLocation}
                      , priceFileType = ${priceFileType} , keyColumn = ${keyColumn}, valueColumn = ${valueColumn}}"""

  val modelsLocation = "\"/project/test/initial-testing/model/models/\""
  val modelSchemasLocation = "\"/project/test/initial-testing/model/schemas/\""

  val instrumentModelSourceConfig = s"instrumentModel{ modelsLocation = ${modelsLocation} , modelSchemasLocation = ${modelSchemasLocation}}"

  val factorsFileLocation = "\"/project/test/initial-testing/\""
  val factorsFileName = "\"factors.clean.may2016.csv\""

  val factorsSourceContents = s"riskFactor{hdfsLocation = ${hdfsLocation}, fileLocation = ${factorsFileLocation}, factorsFileName = ${factorsFileName} }"
  

  
  
  override def beforeAll(): Unit = {

    super.beforeAll()

    // Create a temporary config file to specify the test data to use
    val configFileContents = s"${instrumentPriceSourceConfig}, ${instrumentModelSourceConfig}, ${factorsSourceContents},  }"
    val configFile = writeTempFile(s"${hadoopAppContextEntry}, ${configFileContents}") // Prepend the Hadoop dependencies

    try {
      val result = ApplicationContext.useConfigFile(configFile)
    } finally {
      configFile.delete()
    }

  }

  override def beforeEach() {

    instance = new DefaultInstrumentModelGenerator(sc)
  }

}