package main.scala.factors

import org.apache.spark.sql._
import java.time.LocalDate
import org.apache.spark._
import org.apache.log4j.Logger
import main.scala.application.ApplicationContext
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.functions._
import main.scala.transform.Transformable
import org.apache.spark.ml.Transformer

/**
 * Provide risk factor matrix as a DataFrame from from csv file
 */
case class RiskFactorSourceFromFile(sc: SparkContext) extends RiskFactorSource[DataFrame] with Transformable {

  val appContext = ApplicationContext.getContext

  // Locate data
  val hdfsLocation = appContext.getString("riskFactor.hdfsLocation")
  val fileLocation = appContext.getString("riskFactor.fileLocation")
  val factorsFileName = appContext.getString("riskFactor.factorsFileName")
  //
  private val logger = Logger.getLogger(RiskFactorSourceFromFile.getClass)
  //
  private val sortColumn = "valueDate"
  //
  private val transformers = Vector[Transformer]()
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

  // TEMP. TODO: create Transform objects to handle df manipulation
  private def _transform(df: DataFrame): DataFrame = {

    df.withColumn(s"${sortColumn}Conversion", df(sortColumn).cast(DataTypes.DateType))
      .drop(sortColumn)
      .withColumnRenamed(s"${sortColumn}Conversion", sortColumn)
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
  
  override def add(t:Transformer):Unit = {
    
    // don't allow a null value to be added
    if (t == null) {
      throw new IllegalArgumentException(s"Cannot add a null value")
    }
    transformers  :+ t
  }
  
  override def transform(d:DataFrame): DataFrame = {

    if (transformers.isEmpty) {
      d
    }
    transformers.foldLeft(d)((acc, t) => t.transform(acc))
  }
}