package main.scala.application

import org.apache.spark.sql.SparkSession

/**
 * Substitute for DriverWrapper
 */
object LaunchMonteCarloVaR {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("H-Day MonteCarlo VaR")
      .master("local[3]")
      .getOrCreate()

    MonteCarloVar.main(args)

    spark.sparkContext.stop()
  }
}