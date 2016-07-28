package main.scala.transform

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField

/**
 * Implementation of the Transformer pattern to ensure that a data frame has a column as a Date type
 */
class ValueDateTransformer(override val uid: String) extends Transformer {

  def this() = this(Identifiable.randomUID("vdt"))

  private var _columnName: String = "valueDate"

  def columnName = _columnName

  /**
   * Set the name of the column to be transformed. The default value is "valueDate"
   */
  def columnName(n: String) = {

    if (n == null || n.isEmpty()) {
      throw new IllegalArgumentException(s"The column name was invalid: ${n}")
    }
    _columnName = n
  }

  private val tempColSuffix = Identifiable.randomUID("tempColSuffix")

  override def copy(extra: ParamMap): Transformer = new ValueDateTransformer(uid) // Ignore the params for the time being

  /**
   * Takes the column name and transforms it to a DateType
   */
  override def transform(df: Dataset[_]): DataFrame = {


    df.withColumn(s"${columnName}${tempColSuffix}", df(columnName).cast(DataTypes.DateType))
      .drop(columnName)
      .withColumnRenamed(s"${columnName}${tempColSuffix}", columnName)
  }

  /**
   * Returns the supplied schema with the column name field as a DateType
   */
  override def transformSchema(schema: StructType): StructType = {

    val stableIdentifier = _columnName

    StructType(schema.map { sf =>
      sf match {
        case StructField(`stableIdentifier`, dataType, nullable, metadata) => StructField(`stableIdentifier`, DataTypes.DateType, nullable, metadata)
        case _ => sf
      }
    })
  }
}