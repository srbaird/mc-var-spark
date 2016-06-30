package main.scala.factors

import org.apache.spark.sql._
import java.time.LocalDate
import org.apache.spark._
import org.apache.log4j.Logger
import main.scala.application.ApplicationContext

/**
 * Provide risk factor matrix as a DataFrame from from csv file
 */
case class RiskFactorSourceFromFile(sc: SparkContext) extends RiskFactorSource[DataFrame] {


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
  private lazy val df = readDataFrameFromFile  

  override def head(rows: Int): DataFrame = {
    if (rows < 1) {
      throw new IllegalArgumentException(s"The number of rows must be greater than zero: ${rows}")
    }
    df.sort(sortColumn).limit(rows)
  }

  override def factors(from: LocalDate, to: LocalDate): DataFrame = head

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
}