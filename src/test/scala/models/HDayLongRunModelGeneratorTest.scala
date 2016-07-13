package test.scala.models

import org.apache.spark.sql.SQLContext
import test.scala.application.SparkTestBase
import main.scala.transform.HDayVolatilityTransformer
import main.scala.application.ApplicationContext
import main.scala.models.DefaultInstrumentModelGenerator
import main.scala.prices.InstrumentPriceSourceFromFile
import main.scala.factors.RiskFactorSourceFromFile
import main.scala.models.InstrumentModelSourceFromFile
import main.scala.transform.ValueDateTransformer

class HDayLongRunModelGeneratorTest extends SparkTestBase {

  var instance: DefaultInstrumentModelGenerator = _
  var sqlc: SQLContext = _

  //
  var hdfsLocation: String = _
  var fileLocation: String = _
  var priceFileType: String = _
  var keyColumn: String = _
  var valueColumn: String = _

  var modelsLocation: String = _
  var modelSchemasLocation: String = _

  var factorsFileLocation: String = _
  var factorsFileName: String = _

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

    hdfsLocation = "\"hdfs://localhost:54310\""
    fileLocation = "\"/project/test/initial-testing/prices/\""
    priceFileType = "\".csv\""
    keyColumn = "\"valueDate\""
    valueColumn = "\"closePrice\""

    modelsLocation = "\"/project/test/initial-testing/model/models/\""
    modelSchemasLocation = "\"/project/test/initial-testing/model/schemas/\""

    factorsFileLocation = "\"/project/test/initial-testing/\""
    factorsFileName = "\"factors.clean.csv\""

    hDayValue = "\"10\""
  }

  private def generateContextFileContents: String = {

    val instrumentPriceSourceConfig = s"""instrumentPrice{hdfsLocation = ${hdfsLocation}, fileLocation = ${fileLocation}
                      , priceFileType = ${priceFileType} , keyColumn = ${keyColumn}, valueColumn = ${valueColumn}}"""
    val instrumentModelSourceConfig = s"instrumentModel{ modelsLocation = ${modelsLocation} , modelSchemasLocation = ${modelSchemasLocation}}"

    val factorsSourceContents = s"riskFactor{hdfsLocation = ${hdfsLocation}, fileLocation = ${factorsFileLocation}, factorsFileName = ${factorsFileName} }"
    
    val hDayVolatilityTransformerConfig = s"hDayVolatility{hDayValue = ${hDayValue}}"

    val configFileContents = s"${instrumentPriceSourceConfig}, ${instrumentModelSourceConfig}, ${factorsSourceContents}, ${hDayVolatilityTransformerConfig}"
    s"${hadoopAppContextEntry}, ${configFileContents}" // Prepend the Hadoop dependencies

  }

  private def generateDefaultInstance = {

    instance = new DefaultInstrumentModelGenerator(sc)
    // TODO: move the dependencies to DI implementation
    instance.instrumentModelSource(new InstrumentModelSourceFromFile(sc))

    val instrumentPriceSource = new InstrumentPriceSourceFromFile(sc)
    instrumentPriceSource.add(new ValueDateTransformer)
    instance.instrumentPriceSource(instrumentPriceSource)

    val riskFactorSource = new RiskFactorSourceFromFile(sc)
    riskFactorSource.add(new ValueDateTransformer)
    instance.riskFactorSource(riskFactorSource)

    //
    // Add an HDayVolatilityTransformer to change the type of model generated
    //
    instance.add(new HDayVolatilityTransformer(sc))

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