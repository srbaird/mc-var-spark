package test.scala.models

import main.scala.application.ApplicationContext
import main.scala.models.DefaultInstrumentModelGenerator
import main.scala.models.InstrumentModelSourceFromFile
import test.scala.application.SparkTestBase
import main.scala.prices.InstrumentPriceSourceFromFile
import main.scala.factors.RiskFactorSourceFromFile

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
    val configFileContents = s"${instrumentPriceSourceConfig}, ${instrumentModelSourceConfig}, ${factorsSourceContents}"
    val configFile = writeTempFile(s"${hadoopAppContextEntry}, ${configFileContents}") // Prepend the Hadoop dependencies
    try {
      val result = ApplicationContext.useConfigFile(configFile)
    } finally {
      configFile.delete()
    }

  }
  // Prevent the Spark Context being recycled
  override def beforeEach() {

    instance = new DefaultInstrumentModelGenerator(sc)
    // TODO: move the dependencies to DI implementation
    instance.instrumentModelSource(new InstrumentModelSourceFromFile(sc))
    instance.instrumentPriceSource(new InstrumentPriceSourceFromFile(sc))
    instance.riskFactorSource(new RiskFactorSourceFromFile(sc))
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
}