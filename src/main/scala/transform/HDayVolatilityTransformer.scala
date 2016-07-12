package main.scala.transform

import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.ml.param.ParamMap

class HDayVolatilityTransformer(override val uid: String) extends Transformer {

  def this() = this(Identifiable.randomUID("hdvt"))

  /**
   * Take the DataFrame and return an h-day volatility data set
   */
  override def transform(df: DataFrame): DataFrame = throw new UnsupportedOperationException("Not implemented")

  /**
   * Returns the supplied schema with the column name field as a DateType
   */
  override def transformSchema(schema: StructType): StructType = throw new UnsupportedOperationException("Not implemented")

  override def copy(extra: ParamMap): Transformer = new HDayVolatilityTransformer(uid) // Ignore the params for the time being

}