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
import java.time.LocalDate
import main.scala.transform.DoublesOnlyTransformer

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

  /**
   * Generate a model from the full set of factors/prices over a 250 day period
   */
  test("test generating a valid dataset code between 01-Jun-2015 and 31-May-2016") {

    val expectedDSCode = "TEST_DSNAME_FULL"
    val result = instance.buildModel( LocalDate.of(2015, 6, 1), LocalDate.of(2016, 5, 31), expectedDSCode)
    println(s"Test returned ${result(expectedDSCode)._2} for ${expectedDSCode} ")
   assert(result(expectedDSCode)._1)
   }

  //
  // 
  //
  private def generateContextFileContentValues = {

    hdfsLocation = "\"hdfs://localhost:54310\""
    fileLocation = "\"/project/test/initial-testing/prices/\""
    priceFileType = "\".csv\""
    keyColumn = "\"valueDate\""
    valueColumn = "\"closePrice\""

    modelsLocation = "\"/project/test/initial-testing/h-models/models/\""
    modelSchemasLocation = "\"/project/test/initial-testing/h-models/schemas/\""

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
    // Add a DoublesOnlyTransformer to strip out unused columns for ...
    // ... an HDayVolatilityTransformer to change the type of model generated
    //
    instance.add(new DoublesOnlyTransformer())
    instance.add(new HDayVolatilityTransformer())

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