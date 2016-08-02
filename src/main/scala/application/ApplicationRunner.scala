package main.scala.application

import org.apache.spark.SparkContext

trait ApplicationRunner {
  
  def run(spark:SparkContext, contextFileName:String)
}