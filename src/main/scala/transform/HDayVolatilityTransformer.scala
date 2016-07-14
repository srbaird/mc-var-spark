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
import main.scala.util.Functions._

/**
 * Implementation of the Transformer pattern to reduce a factors data frame to 
 * a data frame of absolute h-day volatility i.e. the difference in value of any data set 
 * over a period of h-days
 */
class HDayVolatilityTransformer(override val uid: String) extends Transformer {

  def this() = this(Identifiable.randomUID("hdvt"))
  //
  //
  //
  private val appContext = ApplicationContext.getContext
  //
  //
  //
  lazy val hDayValue = appContext.getString("hDayVolatility.hDayValue").toInt


  /**
   * Take the DataFrame and return an h-day volatility data frame. The h-value is retrieved from the 
   * application context as 'hDayVolatility.hDayValue'
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
    val sqlc = df.sqlContext
    val sc = sqlc.sparkContext
    sqlc.createDataFrame(sc.parallelize(hDay.map { d => Row.fromSeq(d) }), tSchema)
  }

  /**
   * Returns the supplied schema only if all data types are Double otherwise an IllegalArgumentException
   * is thrown
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

  override def copy(extra: ParamMap): Transformer = new HDayVolatilityTransformer(uid) // Ignore the params for the time being

}