package main.scala.transform

import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.SparkContext

/**
 *
 */
class HDayVolatilityTransformer(sc: SparkContext, override val uid: String) extends Transformer {

  def this(sc: SparkContext) = this(sc, Identifiable.randomUID("hdvt"))

  /**
   * Take the DataFrame and return an h-day volatility data set
   */
  override def transform(df: DataFrame): DataFrame = {

    if (df == null) {
      throw new IllegalArgumentException(s"Invalid data frame supplied: ${df}")
    }
    null
  }

  /**
   * Returns the supplied schema with the column name field as a DateType
   */
  override def transformSchema(schema: StructType): StructType = throw new UnsupportedOperationException("Not implemented")

  override def copy(extra: ParamMap): Transformer = new HDayVolatilityTransformer(sc, uid) // Ignore the params for the time being

}