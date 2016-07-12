package test.scala.transform

import test.scala.application.SparkTestBase
import main.scala.prices.InstrumentPriceSourceFromFile
import main.scala.transform.HDayVolatilityTransformer
import main.scala.factors.RiskFactorSourceFromFile
import org.apache.spark.sql.SQLContext
import main.scala.models.DefaultInstrumentModelGenerator
import main.scala.application.ApplicationContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row
import java.util.ArrayList

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

  /**
   * Invoking transformSchema on a null schema should result in an exception
   */
  test("transform a null schema ") {

    intercept[IllegalArgumentException] {
      instance.transformSchema(null)
    }
  }

  /**
   * Invoking transformSchema with a non-Double DataTypes should result in an exception
   */
  test("transform a schema with an invalid data type ") {

    val invalidSchema = StructType(Array(StructField("InvalidField", DataTypes.DateType)))

    intercept[IllegalArgumentException] {
      instance.transformSchema(invalidSchema)
    }
  }

  /**
   * transformSchema should return the same schema supplied
   *
   */
  test("transform a schema with an valid data type ") {

    val validSchema = StructType(Array(StructField("InvalidField", DataTypes.DoubleType)))

    val result = instance.transformSchema(validSchema)

    assert(result == validSchema)

  }
  
    /**
   * Invoking transform with a data frame generated from an invalid schema should result in an exception
   */
  test("transform a data frame with an invalid schema ") {

    val invalidSchema = StructType(Array(StructField("InvalidField", DataTypes.DateType)))
    val emptyDF = sqlc.createDataFrame(new ArrayList[Row](), invalidSchema)
    
    intercept[IllegalArgumentException] {
      instance.transform(emptyDF)
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
    sqlc = new SQLContext(sc)
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