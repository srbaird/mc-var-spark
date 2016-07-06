package test.scala.models

import main.scala.application.ApplicationContext
import main.scala.models.DefaultInstrumentModelGenerator
import main.scala.models.InstrumentModelSourceFromFile
import test.scala.application.SparkTestBase
import main.scala.prices.InstrumentPriceSourceFromFile
import main.scala.factors.RiskFactorSourceFromFile
import main.scala.transform.ValueDateTransformer

class DefaultInstrumentModelGeneratorTest extends SparkTestBase {

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
  // Prevent the Spark Context being recycled
  override def beforeEach() {

    generateContextFileContentValues

    generateContextFileContents

    generateAppContext

    generateDefaultInstance
  }

  /**
   * Passing a null risk factor source argument should result in an exception
   */
  test("test setting a null risk factor source argument") {

    intercept[IllegalArgumentException] {
      instance.riskFactorSource(null)
    }
  }

  /**
   * Passing a null instrument price source argument should result in an exception
   */
  test("test setting a null instrument price source argument") {

    intercept[IllegalArgumentException] {
      instance.instrumentPriceSource(null)
    }
  }

  /**
   * Passing a null instrument model source argument should result in an exception
   */
  test("test setting a null instrument model source argument") {

    intercept[IllegalArgumentException] {
      instance.instrumentModelSource(null)
    }
  }

  /**
   * Default setup should mean that hasSources is true
   */
  test("test the default setup returns true for hasSources") {

    assert(instance.hasSources)
  }

  /**
   * Setup without an instrument model source argument should mean that hasSources is false
   */
  test("test setup without an instrument model source argument") {

    instance = new DefaultInstrumentModelGenerator(sc)
    instance.instrumentPriceSource(new InstrumentPriceSourceFromFile(sc))
    instance.riskFactorSource(new RiskFactorSourceFromFile(sc))
    assert(!instance.hasSources)
  }

  /**
   * Setup without an instrument model price argument should mean that hasSources is false
   */
  test("test setup without an instrument price source argument") {

    instance = new DefaultInstrumentModelGenerator(sc)
    instance.instrumentModelSource(new InstrumentModelSourceFromFile(sc))
    instance.riskFactorSource(new RiskFactorSourceFromFile(sc))
    assert(!instance.hasSources)
  }

  /**
   * Setup without an risk factor source argument should mean that hasSources is false
   */
  test("test setup without a risk factor source argument") {

    instance = new DefaultInstrumentModelGenerator(sc)
    instance.instrumentPriceSource(new InstrumentPriceSourceFromFile(sc))
    instance.instrumentModelSource(new InstrumentModelSourceFromFile(sc))
    assert(!instance.hasSources)
  }

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

    instance.buildModel("AnyString")
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
    factorsFileName = "\"factors.clean.may2016.csv\""
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