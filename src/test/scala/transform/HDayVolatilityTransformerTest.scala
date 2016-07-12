package test.scala.transform

import test.scala.application.SparkTestBase
import main.scala.prices.InstrumentPriceSourceFromFile
import main.scala.transform.HDayVolatilityTransformer
import main.scala.factors.RiskFactorSourceFromFile
import org.apache.spark.sql.SQLContext
import main.scala.models.DefaultInstrumentModelGenerator
import main.scala.application.ApplicationContext

class HDayVolatilityTransformerTest extends SparkTestBase {

  var instance: HDayVolatilityTransformer = _
  var sqlc: SQLContext = _

  override def beforeAll(): Unit = {

    super.beforeAll()

  }

  private var hDayValue: String = _

  override def beforeEach() {

    generateContextFileContentValues

    generateContextFileContents

    generateAppContext

    generateDefaultInstance
  }

  // Prevent the Spark Context being recycled
  override def afterEach() {}

  /**
   * Invoking transform on a null DataFrame should result in an exception
   */
  test("transform a null data frame ") {

    intercept[IllegalArgumentException] {
      instance.transform(null)
    }
  }
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

    instance = new HDayVolatilityTransformer(sc)
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