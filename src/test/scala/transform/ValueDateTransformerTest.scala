package test.scala.transform

import test.scala.application.SparkTestBase
import org.apache.spark.ml.Transformer
import main.scala.transform.ValueDateTransformer
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataType
import java.time.LocalDate
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row

class ValueDateTransformerTest extends SparkTestBase {

  var instance: ValueDateTransformer = _
  var sqlc: SQLContext = _

  override def beforeAll(): Unit = {

    super.beforeAll()
    // Create the SQL context for data frame generation
    sqlc = new SQLContext(sc)

  }
  override def beforeEach() {

    instance = new ValueDateTransformer()
  }

  /**
   * Test the schema transformation with a single field using the default field name. The
   * resulting schema should be transformed to have a Date date type
   */
  test("Create a schema with a single value date field and test transformSchema ") {

    val fieldName = instance.columnName
    val dataType = DataTypes.StringType
    val expectedDataType = DataTypes.DateType

    val schema = new StructType(Array[StructField](StructField(fieldName, dataType, true)))

    val result = instance.transformSchema(schema)

    assert(result.fieldIndex(fieldName) >= 0)
    assert(result(result.fieldIndex(fieldName)).dataType == expectedDataType)

  }

  /**
   * Test the schema transformation without a field using the default field name. The
   * resulting schema should be not be transformed
   */
  test("Create a schema with no value date field and test that its data type is unchanged ") {

    val fieldName = s"_${instance.columnName}"
    val dataType = DataTypes.StringType
    val expectedDataType = DataTypes.StringType
    val schema = new StructType(Array[StructField](StructField(fieldName, dataType, true)))

    val result = instance.transformSchema(schema)

    assert(result.fieldIndex(fieldName) >= 0)
    assert(result(result.fieldIndex(fieldName)).dataType == expectedDataType)

  }
  /**
   * Test the schema transformation with multiple fields including one using the default field name. Only
   * the value date field should have its data type changed
   */
  test("Create a schema with multiple fields and test transformSchema ") {

    val fieldName = instance.columnName
    val otherFieldName = s"_${fieldName}"

    val dataType = DataTypes.StringType
    val expectedDataType = DataTypes.DateType
    val unchangedDataType = dataType

    val schema = new StructType(
      Array[StructField](
        StructField(s"_${fieldName}", dataType, true),
        StructField(fieldName, dataType, true)))

    val result = instance.transformSchema(schema)

    assert(result(result.fieldIndex(otherFieldName)).dataType == unchangedDataType)
    assert(result(result.fieldIndex(fieldName)).dataType == expectedDataType)
  }

  /**
   * Setting the value date field to an empty string should result in an exception
   */
  test("set the value date field name to an empty string ") {

    intercept[IllegalArgumentException] {
      instance.columnName("")
    }
  }

  /**
   * Setting the value date field to null should result in an exception
   */
  test("set the value date field name to null ") {

    intercept[IllegalArgumentException] {
      instance.columnName(null)
    }
  }

  /**
   * Create a schema with a non-default field name and set the column name in the transformer
   * The resulting schema should be transformed to have a Date date type
   */
  test("set the value date field name and test transformSchema ") {

    val fieldName = "_valueDate"
    val dataType = DataTypes.StringType
    val expectedDataType = DataTypes.DateType

    instance.columnName(fieldName)
    val schema = new StructType(Array[StructField](StructField(fieldName, dataType, true)))

    val result = instance.transformSchema(schema)

    assert(result.fieldIndex(fieldName) >= 0)
    assert(result(result.fieldIndex(fieldName)).dataType == expectedDataType)
  }
  
  /**
   * Transform an empty data frame should result in no rows being returned
   * The resulting schema should be transformed to have a Date date type
   */
  test("transform on empty data frame ") {
    
    val result = instance.transform(createTestDataFrame(0))
    assert(result.count() == 0)

  }

  // 
  // Helper methods
  //
  private def createTestDataFrame(nRows: Int): DataFrame = {

    val fieldName = instance.columnName
    val stringType = DataTypes.StringType
    val schema = new StructType(
      Array[StructField](
        StructField(s"_${fieldName}", stringType, true),
        StructField(fieldName, stringType, true)))

    val col1Value = "Not important"
    val col2Value = LocalDate.now().toString()
    val rowData = Array[String](col1Value, col2Value)

    val rows = new Array[Row](nRows)

    for (i <- 0 to nRows) {
      rows(i) = Row.fromSeq(rowData)
    }
    sqlc.createDataFrame(sc.parallelize(rows), StructType(schema))
  }

}