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

    val validSchema = StructType(Array(StructField("InvalidField", DataTypes.DateType)))
    val emptyDF = sqlc.createDataFrame(new ArrayList[Row](), validSchema)

    intercept[IllegalArgumentException] {
      instance.transform(emptyDF)
    }
  }

  /**
   * Invoking transform with an h-day value less than 1 should result in an exception
   */
  test("transform with an h-day value less than 1 ") {

    val validSchema = StructType(Array(StructField("ValidField", DataTypes.DoubleType)))

    val dfRows = Array(Row(2D), Row(3D))
    val testDF = sqlc.createDataFrame(sc.parallelize(dfRows), StructType(validSchema))

    // Set the h-day value to 0    
    hDayValue = "\"0\""

    // Reset the test environment
    resetTestEnvironment

    intercept[IllegalArgumentException] {
      instance.transform(testDF)
    }
  }

  /**
   * Invoking transform with an h-day value too high for the size of data frame should result in an exception
   */
  test("transform a data frame smaller than hDay + 1 ") {

    val invalidSchema = StructType(Array(StructField("ValidField", DataTypes.DoubleType)))
    val emptyDF = sqlc.createDataFrame(new ArrayList[Row](), invalidSchema)

    intercept[IllegalArgumentException] {
      instance.transform(emptyDF)
    }
  }

  /**
   * Transform a single column 2-row data frame into a 1 row data frame
   */
  test("transform a single column 2-row data frame into a single row data frame") {

    val validSchema = StructType(Array(StructField("SingleColumn", DataTypes.DoubleType)))

    val dfRows = Array(Row(2D), Row(3D))
    val testDF = sqlc.createDataFrame(sc.parallelize(dfRows), StructType(validSchema))

    // Set the h-day value to 1    
    hDayValue = "\"1\""

    // Reset the test environment
    resetTestEnvironment

    val result = instance.transform(testDF)

    assert(result.count() == 1)
    val expectedColValue = 1 // i.e. 3 - 2
    val resultColValue = result.head().getDouble(0)
    assert(expectedColValue == resultColValue)
  }

  /**
   * Transform a 2 column 11-row data frame into a 1 row data frame
   */
  test("transform a 2 column 11-row data frame into a single row data frame") {

    val validSchema = StructType(Array(StructField("FirstColumn", DataTypes.DoubleType), StructField("SecondColumn", DataTypes.DoubleType)))

    val dfRows = Array(Row.fromSeq(Array(1D, 100D)), Row.fromSeq(Array(2D, 99D)), Row.fromSeq(Array(3D, 98D)), Row.fromSeq(Array(4D, 97D)), Row.fromSeq(Array(5D, 96D)), Row.fromSeq(Array(6D, 95D)), Row.fromSeq(Array(7D, 94D)), Row.fromSeq(Array(8D, 93D)), Row.fromSeq(Array(9D, 92D)), Row.fromSeq(Array(10D, 91D)), Row.fromSeq(Array(11D, 90D)))

    val testDF = sqlc.createDataFrame(sc.parallelize(dfRows), StructType(validSchema))

    val result = instance.transform(testDF)

    assert(result.count() == 1)
    val expectedCol1Value = 10 // i.e. 11 - 1
    val expectedCol2Value = -10 // i.e. 90 - 100
    val resultCol1Value = result.head().getDouble(0)
    val resultCol2Value = result.head().getDouble(1)
    assert(expectedCol1Value == resultCol1Value)
    assert(expectedCol2Value == resultCol2Value)
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

    instance = new HDayVolatilityTransformer()
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

  private def resetTestEnvironment = {

    generateContextFileContents
    generateAppContext
    generateDefaultInstance
  }
}