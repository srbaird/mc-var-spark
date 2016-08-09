package main.scala.factors

import java.time.LocalDate

import scala.Vector

import org.apache.log4j.Logger
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.annotation.Experimental
import org.apache.spark.annotation.Since
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.desc

import main.scala.application.ApplicationContext
import main.scala.transform.Transformable

/**
 * Provide risk factor matrix as a DataFrame from from csv file
 */
case class RiskFactorSourceFromFile(val t: Array[Transformer]) extends RiskFactorSource[DataFrame] {

  // Ensure a non-null sequence of transformers  
  def this() = this(Array[Transformer]())

  val appContext = ApplicationContext.getContext

  val sc = ApplicationContext.sc

  // Locate data
  lazy val fileLocation = appContext.getString("riskFactor.fileLocation")
  lazy val factorsFileName = appContext.getString("riskFactor.factorsFileName")
  //
  //
  //
  private val logger = Logger.getLogger(getClass)
  //
  // TODO: Move this to the context file
  //
  private val sortColumn = "valueDate"
  //
  private lazy val df = transform(readDataFrameFromFile)

  /**
   * A positive non-zero number of rows must be supplied. The data is first sorted to ensure the most recent rows are returned
   */
  override def head(rows: Int): DataFrame = {

    logger.debug(s"Get first ${rows} rows")
    if (rows < 1) {
      throw new IllegalArgumentException(s"The number of rows must be greater than zero: ${rows}")
    }
    df.sort(desc(sortColumn)).limit(rows)
  }

  override def factors(): DataFrame = df

  /**
   * From-date must not be greater than to date. If to-date is not supplied then all rows from the start date will be selected
   */
  override def factors(from: LocalDate, to: LocalDate = null): DataFrame = {

    logger.debug(s"Get factors between ${from} and ${to}")
    if (from == null) {
      throw new IllegalArgumentException(s"An invalid start date was supplied: ${from}")
    }

    if (to != null) {

      if (from.compareTo(to) > 0) {
        throw new IllegalArgumentException(s"The from date exceeded the to date: ${from}")
      } else {

        val fromDate = java.sql.Date.valueOf(from)
        val toDate = java.sql.Date.valueOf(to)
        df.filter(df(sortColumn).geq(fromDate)).filter(df(sortColumn).leq(toDate))
      }
    } else {

      val fromDate = java.sql.Date.valueOf(from)
      df.filter(df(sortColumn).geq(fromDate))
    }
  }

  private def readDataFrameFromFile: DataFrame = {

    // Use the DataBricks implementation
    val csvReadFormat = "com.databricks.spark.csv"
    val hdfsLocation = ApplicationContext.getHadoopConfig.get("fs.default.name")
    val fileURI = s"${hdfsLocation}${fileLocation}${factorsFileName}"

    logger.trace(s"Load dataframe from '${fileURI}'")

    val sqlc = new SQLContext(sc)

    sqlc.read
      .format(csvReadFormat)
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(fileURI)

  }

  def transform(d: DataFrame): DataFrame = {
    
    logger.trace(s"Perform transform on ${d}")
    if (t.isEmpty) {
      d
    } else {
      t.foldLeft(d)((acc, t) => t.transform(acc))
    }
  }
}