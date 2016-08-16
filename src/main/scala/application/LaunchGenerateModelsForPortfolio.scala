package main.scala.application

import org.apache.spark.sql.SparkSession

object LaunchGenerateModelsForPortfolio extends PreLoadHadoopConfig {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("Build Prediction Models")
      .master("local[3]")
      .getOrCreate()

    load

    GenerateModelsForPortfolio.main(args)

    spark.sparkContext.stop()
  }
  
}