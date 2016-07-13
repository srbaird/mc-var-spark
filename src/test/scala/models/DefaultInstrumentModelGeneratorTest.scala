package test.scala.models

import main.scala.application.ApplicationContext
import main.scala.models.DefaultInstrumentModelGenerator
import main.scala.models.InstrumentModelSourceFromFile
import test.scala.application.SparkTestBase
import main.scala.prices.InstrumentPriceSourceFromFile
import main.scala.factors.RiskFactorSourceFromFile
import main.scala.transform.ValueDateTransformer
import java.time.LocalDate

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

  override def beforeEach() {

    generateContextFileContentValues

    generateContextFileContents

    generateAppContext

    generateDefaultInstance
  }

  // Prevent the Spark Context being recycled
  override def afterEach() {}

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