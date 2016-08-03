package main.scala.application

import org.apache.spark.sql.SparkSession

object LaunchGenerateObservations extends PreLoadHadoopConfig {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("Generate Portfolio Variance Observations")
      .master("local[3]")
      .getOrCreate()

    load
        
    GenerateObservations.main(args)

    spark.sparkContext.stop()
  }
}