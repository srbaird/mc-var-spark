package main.scala.application

import org.apache.spark.sql.SparkSession

object LaunchCovarianceVaR {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("H-Day Covariance VaR")
      .master("local[3]")
      .getOrCreate()

    CovarianceVar.main(args)

    spark.sparkContext.stop()
  }
}