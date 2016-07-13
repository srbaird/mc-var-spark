package test.scala.transform

import main.scala.transform.DoublesOnlyTransformer
import test.scala.application.SparkTestBase
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataTypes
import main.scala.application.ApplicationContext
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row
import java.util.ArrayList
import org.apache.spark.sql.SQLContext

class DoublesOnlyTransformerTest extends SparkTestBase {

  var instance: DoublesOnlyTransformer = _
  var sqlc: SQLContext = _

  override def beforeAll(): Unit = {

    super.beforeAll()
  }

  override def beforeEach() {

    generateContextFileContentValues
    resetTestEnvironment
    sqlc = new SQLContext(sc)
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
   * Invoking transformSchema on an empty should return an empty schema
   */
  test("transform an empty schema ") {

    val emptySchema = StructType(Array[StructField]())

    val result = instance.transformSchema(emptySchema)
    assert(result.isEmpty)
  }

  /**
   * Invoking transformSchema on a schema with all non Double data types
   * should return an empty schema
   */
  test("transform an all non-Double schema ") {

    val emptySchema = StructType(Array(
      StructField("InvalidField1", DataTypes.DateType),
      StructField("InvalidField2", DataTypes.DateType)))

    val result = instance.transformSchema(emptySchema)
    assert(result.isEmpty)

  }

  /**
   * Invoking transformSchema on a schema with mixed data types
   * should return a schema with only DoubleTypes
   */
  test("transform a mixed data type schema ") {

    val validDataType = DataTypes.DoubleType

    val nonEmptySchema = StructType(Array(
      StructField("ValidField", validDataType),
      StructField("InvalidField", DataTypes.DateType)))

    val result = instance.transformSchema(nonEmptySchema)
    assert(result.length == 1)
    assert(result(0).dataType == validDataType)
  }

  /**
   * Invoking transform on an empty dataframe should return an empty dataframe
   */
  test("transform an empty dataframe ") {

    val emptySchema = StructType(Array[StructField]())

    val result = instance.transform(sqlc.emptyDataFrame)
    assert(result.count() == 0)
  }

  /**
   * Invoking transform on a dataframe with an all non Double data types
   * should return an empty dataframe
   */
  test("transform an all non-Double data types dataframe ") {

    val invalidSchema = StructType(Array(
      StructField("InvalidField1", DataTypes.DateType),
      StructField("InvalidField2", DataTypes.DateType)))

    val dfRows = Array(Row.fromSeq(Array("", "")))

    val testDF = sqlc.createDataFrame(sc.parallelize(dfRows), StructType(invalidSchema))

    val result = instance.transform(testDF)
    assert(result.count() == 0)
  }

  /**
   * Invoking transform on a dataframe with mixed data types
   * should return a dataframe with only DoubleTypes
   */
  test("transform a mixed data types dataframe ") {

    val validDataType = DataTypes.DoubleType
    val expectedValue = 99D

    val invalidSchema = StructType(Array(
      StructField("InvalidField1", DataTypes.StringType),
      StructField("ValidField", validDataType),
      StructField("InvalidField2", DataTypes.StringType)))

    val dfRows = Array(Row.fromSeq(Array("", expectedValue, "")))

    val testDF = sqlc.createDataFrame(sc.parallelize(dfRows), StructType(invalidSchema))

    val result = instance.transform(testDF)
    assert(result.count() == 1)
    assert(result.schema.length == 1)
    assert(result.head().getDouble(0) == expectedValue)
  }
  //
  //
  //
  private def generateContextFileContentValues = {

    //   hDayValue = "\"10\""

  }

  private def generateContextFileContents: String = {

    //   val hDayVolatilityTransformerConfig = s"hDayVolatility{hDayValue = ${hDayValue}}"
    //   s"${hadoopAppContextEntry}, ${hDayVolatilityTransformerConfig}" // Prepend the Hadoop dependencies
    ""
  }

  private def generateDefaultInstance = {

    instance = new DoublesOnlyTransformer()

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