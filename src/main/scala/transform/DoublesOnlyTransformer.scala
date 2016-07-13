package main.scala.transform

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataTypes

/**
 * Remove all columns that are not of type Double
 */
class DoublesOnlyTransformer(override val uid: String) extends Transformer {

  def this() = this(Identifiable.randomUID("dnlyt"))

  override def copy(extra: ParamMap): Transformer = new ValueDateTransformer(uid) // Ignore the params for the time being

  /**
   * Select only the valid columns from the supplied data frame
   */
  override def transform(df: DataFrame): DataFrame = {

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
   * Filter out any Struct fields not of the appropriate type
   */
  override def transformSchema(schema: StructType): StructType = {

    if (schema == null) {
      throw new IllegalArgumentException(s"Invalid schema supplied: ${schema}")
    }

    val validDataType = DataTypes.DoubleType
    StructType(schema.filter { _.dataType == validDataType })
  }
}