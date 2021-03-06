package main.scala.portfolios

import java.time.LocalDate

import scala.Vector

import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.types.DataTypes

import main.scala.application.ApplicationContext
import main.scala.transform.Transformable

class PortfolioValuesSourceFromFile(val t: Seq[Transformer]) extends PortfolioValuesSource[DataFrame] {

  // Ensure a non-null sequence of transformers 
  def this() = this(Array[Transformer]())

  val appContext = ApplicationContext.getContext

  val sc = ApplicationContext.sc
  //
  // Context variables to locate the data
  //
  lazy val fileLocation = appContext.getString("portfolioHolding.fileLocation")
  lazy val portfolioFileType = appContext.getString("portfolioHolding.portfolioFileType")
  lazy val keyColumn = appContext.getString("portfolioHolding.keyColumn")
  lazy val valueColumn = appContext.getString("portfolioHolding.valueColumn")
  lazy val instrumentColumn = appContext.getString("portfolioHolding.instrumentColumn")
  //
  private val logger = Logger.getLogger(getClass)


  override def getAvailableCodes(): Seq[String] = {
    
    logger.debug(s"Get available portfolio codes ")
    
    // Use the Hadoop configuration from the Application Context rather than the Spark default
    val fs = FileSystem.get(ApplicationContext.getHadoopConfig)

    val p = new Path(fileLocation)
    val files = fs.listFiles(p, false)
    var found = Array[String]()

    while (files.hasNext()) {
      val f = files.next().getPath.getName
      if (f.endsWith(portfolioFileType)) {
        found = found :+ FilenameUtils.removeExtension(f)
      }
    }
    found
  }

  override def getHoldings(portfolioCode: String, at: LocalDate): DataFrame = {

    logger.debug(s"Get the holdings for ${portfolioCode} at ${at} ")
    
    if (portfolioCode == null || portfolioCode.isEmpty()) {
      throw new IllegalArgumentException(s"An invalid portfolio code was supplied: ${portfolioCode}")
    }
    if (at == null) {
      throw new IllegalArgumentException(s"An invalid value at date was supplied: ${at}")
    }
    if (!getAvailableCodes().contains(portfolioCode)) {
      throw new IllegalStateException(s"No portfolio holdings exist for the given code: ${portfolioCode}")
    }
    // Load the appropriate file
    // Use the DataBricks implementation

    val hdfsContext = ApplicationContext.getHadoopConfig
    val hdfsLocation = hdfsContext.getTrimmed("fs.default.name")
    val csvReadFormat = "com.databricks.spark.csv"
    val fileURI = s"${hdfsLocation}${fileLocation}${portfolioCode}${portfolioFileType}"
    val sqlc = new SQLContext(sc)

    val df = sqlc.read
      .format(csvReadFormat)
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(fileURI)

    val atDate = java.sql.Date.valueOf(at)
    // Create a temp table (!) containing a single row for each instrument
    // Uses the maximum value date that is less than or equal to the at-date

    val dfJoin = df.filter((df(keyColumn).cast(DataTypes.DateType)).leq(atDate)) // remove future dates
      .groupBy(instrumentColumn) // for each instrument...
      .agg(max(df.col(keyColumn))) // ... get the maximum date
      .withColumnRenamed(s"max(${keyColumn})", keyColumn) // rename the columns back to their original values 

    transform(df.join(dfJoin, dfJoin.columns)) // Join the dataset to the temp table 
  }



  /**
   * Apply available transformers in sequence
   */
  def transform(d: DataFrame): DataFrame = {

    logger.trace(s"perform transformation on ${d} ")
    
    if (t.isEmpty) {
      d
    } else {
      t.foldLeft(d)((acc, t) => t.transform(acc))
    }
  }
}