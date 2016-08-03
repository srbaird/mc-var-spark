package main.scala.application

import org.apache.spark.sql.SparkSession

object LaunchCovarianceVaR extends PreLoadHadoopConfig {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("H-Day Covariance VaR")
      .master("local[3]")
      .getOrCreate()

    load
    
    CovarianceVar.main(args)

    spark.sparkContext.stop()
  }
}