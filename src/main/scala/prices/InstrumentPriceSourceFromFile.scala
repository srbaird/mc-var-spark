package main.scala.prices

import java.time.LocalDate
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.DataFrame
import main.scala.application.ApplicationContext
import main.scala.factors.RiskFactorSourceFromFile
import main.scala.transform.Transformable
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.sql.SQLContext

/**
 * File backed implementation of InstrumentPriceSource which generates date-price pairs as a DataFrame
 */
class InstrumentPriceSourceFromFile() extends InstrumentPriceSource[DataFrame] with Transformable {

  val appContext = ApplicationContext.getContext 
  
  val sc = ApplicationContext.sc

  // Locate data
  val hdfsLocation = appContext.getString("fs.default.name")
  val fileLocation = appContext.getString("instrumentPrice.fileLocation")
  val priceFileType = appContext.getString("instrumentPrice.priceFileType")
  //
  private val logger = Logger.getLogger(RiskFactorSourceFromFile.getClass)
  //
  private val keyColumn = appContext.getString("instrumentPrice.keyColumn")
  private val valueColumn = appContext.getString("instrumentPrice.valueColumn")
  //
  private var transformers = Vector[Transformer]()
  //
  /**
   * Supplies all available prices for a given instrument code
   */
  override def getPrices(dsCode: String): DataFrame = {

    if (dsCode == null || dsCode.isEmpty) {
      throw new IllegalArgumentException(s"An invalid dataset code date was supplied: ${dsCode}")
    }

    // Use the DataBricks implementation
    val csvReadFormat = "com.databricks.spark.csv"
    val fileURI = s"${hdfsLocation}${fileLocation}${dsCode}${priceFileType}"

    logger.debug(s"Load price file from '${fileURI}'")

    val sqlc = new SQLContext(sc)

    val df = sqlc.read
      .format(csvReadFormat)
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(fileURI)

    transform(df.select(keyColumn, valueColumn)) // Apply the supplied transformations
  }

  /**
   * Supplies all available prices for a given instrument code between two dates
   */
  override def getPrices(dsCode: String, from: LocalDate, to: LocalDate): DataFrame = {

    if (dsCode == null || dsCode.isEmpty) {
      throw new IllegalArgumentException(s"An invalid dataset code date was supplied: ${dsCode}")
    }

    if (from == null) {
      throw new IllegalArgumentException(s"An invalid start date was supplied: ${from}")
    }

    if (to != null) {

      if (from.compareTo(to) > 0) {
        throw new IllegalArgumentException(s"The from date ${from} exceeded the to date: ${to}")
      } else {
        val df = getPrices(dsCode)
        val fromDate = java.sql.Date.valueOf(from)
        val toDate = java.sql.Date.valueOf(to)
        df.filter(df(keyColumn).geq(fromDate)).filter(df(keyColumn).leq(toDate))
      }
    } else {
      val fromDate = java.sql.Date.valueOf(from)
      val df = getPrices(dsCode)
      df.filter(df(keyColumn).geq(fromDate))
    }
  }

  /**
   * Provide a list of all available instrument codes
   */
  def getAvailableCodes(): Seq[String] = {

    // Use the Hadoop configuration from the Application Context rather than the Spark default
    val fs = FileSystem.get(ApplicationContext.getHadoopConfig)

    val p = new Path(fileLocation)
    val files = fs.listFiles(p, false)
    var found = Array[String]()

    while (files.hasNext()) {
      val f = files.next().getPath.getName
      if (f.endsWith(priceFileType)) {
        found = found :+ FilenameUtils.removeExtension(f)
      }
    }
    found
  }

  /**
   * Add a Transformer to the sequence
   */
  override def add(t: Transformer): Unit = {

    // don't allow a null value to be added
    if (t == null) {
      throw new IllegalArgumentException(s"Cannot add a null value")
    }
    transformers = transformers :+ t
  }

  /**
   * Apply available transformers in sequence
   */
  override def transform(d: DataFrame): DataFrame = {

    if (transformers.isEmpty) {
      d
    } else {
      transformers.foldLeft(d)((acc, t) => t.transform(acc))
    }
  }

}