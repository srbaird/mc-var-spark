package main.scala.transform

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.Dataset
import org.apache.log4j.Logger

/**
 * Remove all columns that are not of type Double
 */
class DoublesOnlyTransformer(override val uid: String) extends Transformer {

  def this() = this(Identifiable.randomUID("dnlyt"))
  //
  //
  //
  private val logger = Logger.getLogger(getClass)

  override def copy(extra: ParamMap): Transformer = new ValueDateTransformer(uid) // Ignore the params for the time being

  /**
   * Select only the valid columns from the supplied data frame
   */
  override def transform(df: Dataset[_]): DataFrame = {

    logger.trace(s"Transform data set: ${df}")
    if (df == null) {
      throw new IllegalArgumentException(s"Invalid data frame supplied: ${df}")
    }

    val dCols = transformSchema(df.schema).fieldNames

    if (df.schema.isEmpty || dCols.isEmpty) {
      df.sqlContext.emptyDataFrame
    } else {
      df.select(dCols.head, dCols.tail: _*)
    }
  }

  /**
   * Filter out any Struct fields not of the appropriate type (integer types are allowed to be parsed)
   */
  override def transformSchema(schema: StructType): StructType = {

    logger.trace(s"Transform schema: ${schema}")
    if (schema == null) {
      throw new IllegalArgumentException(s"Invalid schema supplied: ${schema}")
    }
    val doubleDataType = DataTypes.DoubleType
    val integerDataType = DataTypes.IntegerType
    StructType(schema.filter { s => s.dataType == doubleDataType || s.dataType == integerDataType })
  }
}