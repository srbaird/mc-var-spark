package test.scala.models

import main.scala.application.ApplicationContext
import main.scala.models.DefaultInstrumentModelGenerator
import main.scala.models.InstrumentModelSourceFromFile
import test.scala.application.SparkTestBase
import main.scala.prices.InstrumentPriceSourceFromFile
import main.scala.factors.RiskFactorSourceFromFile
import main.scala.transform.ValueDateTransformer

class LongRunDefaultInstrumentModelGeneratorTest extends SparkTestBase {

  var instance: DefaultInstrumentModelGenerator = _
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

  override def beforeAll(): Unit = {

    super.beforeAll()

  }

  override def beforeEach() {

    generateContextFileContentValues

    generateContextFileContents

    generateAppContext

    generateDefaultInstance
  }

  // Prevent the Spark Context being recycled
  override def afterEach() {}

  /**
   * Generating a model with a null dataset code should result in an exception
   */
  test("test generating model with null dataset code") {

    intercept[IllegalArgumentException] {
      instance.buildModel(null)
    }
  }

  /**
   * Generating a model with an empty dataset code should result in an exception
   */
  test("test generating model with an empty dataset code") {

    intercept[IllegalArgumentException] {
      instance.buildModel("")
    }
  }

  /**
   * Generating a model without required dependencies should result in an exception
   */
  test("test generating model without setting dependencies") {

    instance = new DefaultInstrumentModelGenerator(sc)
    intercept[IllegalStateException] {
      instance.buildModel("AnyString")
    }
  }

  /**
   * Generating a model with an empty factors file
   */
  test("test generating model without risk factors data") {

    factorsFileName = "\"factors.clean.empty.csv\""
    generateContextFileContents
    generateAppContext
    generateDefaultInstance

    val expectedDSCode = "AnyString"
    val result = instance.buildModel(expectedDSCode)
    assert(!result(expectedDSCode)._1)
  }

  /**
   * Generating a model with no dataset code prices
   */
  test("test generating model without dataset price data") {

    val expectedDSCode = "AnyString"
    val result = instance.buildModel(expectedDSCode)
    assert(!result(expectedDSCode)._1)

  }

  /**
   * Generating a model with existing code prices
   */
  test("test generating model with existing dataset price data") {

    val availableCodes = new InstrumentPriceSourceFromFile(sc).getAvailableCodes()

    val expectedDSCode = "TEST_DSNAME_FULL" // 
    assert(availableCodes.contains(expectedDSCode))

    // Remove the model
    val instrumentModelSource = new InstrumentModelSourceFromFile(sc)
    instrumentModelSource.removeModel(expectedDSCode)
    assert(!instrumentModelSource.getAvailableModels.contains(expectedDSCode))

    val result = instance.buildModel(expectedDSCode)
    println(s"Received: ${result(expectedDSCode)._2}")
    assert(result(expectedDSCode)._1)

    assert(instrumentModelSource.getAvailableModels.contains(expectedDSCode))
  }

  /**
   * Generating a model with existing code prices
   */
  test("test generating two models with existing dataset price data") {

    val availableCodes = new InstrumentPriceSourceFromFile(sc).getAvailableCodes()

    val expectedDSCode1 = "TEST_DSNAME_FULL" // 
    val expectedDSCode2 = "TEST_DSNAME_FULL2" // 
    assert(availableCodes.contains(expectedDSCode1))
    assert(availableCodes.contains(expectedDSCode2))

    // Remove the models
    val instrumentModelSource = new InstrumentModelSourceFromFile(sc)
    instrumentModelSource.removeModel(expectedDSCode1)
    instrumentModelSource.removeModel(expectedDSCode2)
    assert(!instrumentModelSource.getAvailableModels.contains(expectedDSCode1))
    assert(!instrumentModelSource.getAvailableModels.contains(expectedDSCode2))

    val result = instance.buildModel(expectedDSCode1, expectedDSCode2)
    
    println(s"Received: ${result(expectedDSCode1)._2}")
    assert(result(expectedDSCode1)._1)
    println(s"Received: ${result(expectedDSCode2)._2}")
    assert(result(expectedDSCode2)._1)

    assert(instrumentModelSource.getAvailableModels.contains(expectedDSCode1))
    assert(instrumentModelSource.getAvailableModels.contains(expectedDSCode2))
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

    modelsLocation = "\"/project/test/initial-testing/model/models/\""
    modelSchemasLocation = "\"/project/test/initial-testing/model/schemas/\""

    factorsFileLocation = "\"/project/test/initial-testing/\""
    factorsFileName = "\"factors.clean.csv\""
  }

  private def generateContextFileContents: String = {

    val instrumentPriceSourceConfig = s"""instrumentPrice{hdfsLocation = ${hdfsLocation}, fileLocation = ${fileLocation}
                      , priceFileType = ${priceFileType} , keyColumn = ${keyColumn}, valueColumn = ${valueColumn}}"""
    val instrumentModelSourceConfig = s"instrumentModel{ modelsLocation = ${modelsLocation} , modelSchemasLocation = ${modelSchemasLocation}}"

    val factorsSourceContents = s"riskFactor{hdfsLocation = ${hdfsLocation}, fileLocation = ${factorsFileLocation}, factorsFileName = ${factorsFileName} }"
    val configFileContents = s"${instrumentPriceSourceConfig}, ${instrumentModelSourceConfig}, ${factorsSourceContents}"
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

  }

  private def generateAppContext {

    val configFile = writeTempFile(generateContextFileContents)
    try {
      val result = ApplicationContext.useConfigFile(configFile)
    } finally {
      configFile.delete()
    }
  }

}