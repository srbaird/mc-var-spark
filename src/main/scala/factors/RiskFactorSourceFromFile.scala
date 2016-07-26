package main.scala.factors

import java.time.LocalDate

import scala.Vector

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.desc

import main.scala.application.ApplicationContext
import main.scala.transform.Transformable

/**
 * Provide risk factor matrix as a DataFrame from from csv file
 */
case class RiskFactorSourceFromFile(val t:Array[Transformer]) extends RiskFactorSource[DataFrame] with Transformable {
    
  // Ensure a non-null sequence of transformers  
  def this() = this(Array[Transformer]())

  val appContext = ApplicationContext.getContext 
  
  val sc = ApplicationContext.sc

  // Locate data
  lazy val hdfsLocation = appContext.getString("fs.default.name") 
  lazy val fileLocation = appContext.getString("riskFactor.fileLocation")
  lazy val factorsFileName = appContext.getString("riskFactor.factorsFileName")
  //
  private val logger = Logger.getLogger(RiskFactorSourceFromFile.getClass)
  //
  private val sortColumn = "valueDate"
  //
  private var transformers = Vector[Transformer]()
  //
  private lazy val df = transform(readDataFrameFromFile)

  /**
   * A positive non-zero number of rows must be supplied. The data is first sorted to ensure the most recent rows are returned
   */
  override def head(rows: Int): DataFrame = {
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

  def readDataFrameFromFile: DataFrame = {

    // Use the DataBricks implementation
    val csvReadFormat = "com.databricks.spark.csv"
    val fileURI = s"${hdfsLocation}${fileLocation}${factorsFileName}"

    logger.debug(s"Load dataframe from '${fileURI}'")

    val sqlc = new SQLContext(sc)

    sqlc.read
      .format(csvReadFormat)
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(fileURI)

  }

  override def add(t: Transformer): Unit = {

    // don't allow a null value to be added
    if (t == null) {
      throw new IllegalArgumentException(s"Cannot add a null value")
    }
    transformers = transformers :+ t
  }

  override def transform(d: DataFrame): DataFrame = {

    if (t.isEmpty) {
      d
    } else {
      t.foldLeft(d)((acc, t) => t.transform(acc))
    }
  }
}