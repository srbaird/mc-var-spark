package main.scala.application

import org.apache.spark.sql.SparkSession

/**
 * Substitute for DriverWrapper
 */
object LaunchMonteCarloVaR extends PreLoadHadoopConfig {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("H-Day MonteCarlo VaR")
      .master("local[3]")
//      .master("local")
      .getOrCreate()

    load

    MonteCarloVar.main(args)

    spark.sparkContext.stop()
  }
}