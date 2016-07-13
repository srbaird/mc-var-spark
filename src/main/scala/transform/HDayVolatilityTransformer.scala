package main.scala.transform

import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataTypes
import main.scala.application.ApplicationContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
/**
 *
 */
class HDayVolatilityTransformer(sc: SparkContext, override val uid: String) extends Transformer {

  def this(sc: SparkContext) = this(sc, Identifiable.randomUID("hdvt"))
  //
  //
  //
  val sqlc = new SQLContext(sc)
  //
  //
  //
  private val appContext = ApplicationContext.getContext
  //
  //
  //
  lazy val hDayValue = appContext.getString("hDayVolatility.hDayValue").toInt

  // Implicit and Explicit conversions to Double
  // From https://gist.github.com/frgomes/c6bf34eeb5ae1769b072 -  Added String
  //
  def toDouble: (Any) => Double = { case i: Int => i case f: Float => f case d: Double => d case s: String => s.toDouble }

  //
  // Apply a function by column over a window onto a 2D matrix of doubles
  //
  def window(s: Int, m: Array[Array[Double]], f: Seq[Double] => Double): Array[Array[Double]] = {
    m.transpose.map { r => r.sliding(s).map { w => f(w) }.toArray }.transpose
  }
  //
  // Collect a DataFrame into an 2D Array of Doubles
  //
  def dfToArrayMatrix(df: DataFrame): Array[Array[Double]] = {

    df.collect.toArray.map { row => row.toSeq.toArray.map { x => toDouble(x).asInstanceOf[Double] } }
  }

  /**
   * Take the DataFrame and return an h-day volatility data frame
   */
  override def transform(df: DataFrame): DataFrame = {

    if (df == null) {
      throw new IllegalArgumentException(s"Invalid data frame supplied: ${df}")
    }
    // Number of rows must be greater than zero and exceed the h-day range
    if (!(hDayValue > 0 ) || !(df.count > hDayValue)) {
      throw new IllegalArgumentException(s"Insufficient rows (${df.count}) in data frame for h-day value ${hDayValue}")
    }
    //
    // Validate the schema
    //
    val tSchema = transformSchema(df.schema)
    //
    // Create a matrix from the supplied rows
    //
    val dfRowsAsMatrix = dfToArrayMatrix(df)
    //
    //  Calculate the volatility (old value - new value). 
    //  Note that the window is the h-day value plus 1
    //
    val hDay = window(hDayValue + 1, dfRowsAsMatrix, w => w.last - w.head)
    //
    // Return as a data frame
    //
    sqlc.createDataFrame(sc.parallelize(hDay.map { d => Row.fromSeq(d) }), tSchema)
  }

  /**
   * Returns the supplied schema with the column name field as a DateType
   */
  override def transformSchema(schema: StructType): StructType = {

    if (schema == null) {
      throw new IllegalArgumentException(s"Invalid schema supplied: ${schema}")
    }
    val validDataType = DataTypes.DoubleType

    StructType(schema.map { sf =>
      sf match {
        case StructField(name, `validDataType`, nullable, metadata) => sf
        case _ => throw new IllegalArgumentException(s"Schema contains invalid data type")
      }
    })
  }

  override def copy(extra: ParamMap): Transformer = new HDayVolatilityTransformer(sc, uid) // Ignore the params for the time being

}