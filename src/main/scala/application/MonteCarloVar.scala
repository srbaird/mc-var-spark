package main.scala.application

import java.io.File
import java.net.URL
import java.time.LocalDate
import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader
import org.springframework.context.support.GenericApplicationContext
import org.springframework.core.io.UrlResource
import main.scala.predict.PredictionPersistor
import org.apache.spark.sql.SparkSession
import main.scala.predict.ValueGenerator

object MonteCarloVar {

  def main(args: Array[String]) {

    // TODO: Implement logging correctly
    Logger.getLogger(classOf[RackResolver]).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    
    println(s"Invoked ${getClass.getSimpleName} with '${args.mkString(", ")}'")

    // TODO: Identify and process passed arguments
    run
    println("Completed run")
  }

  private def run = {

    val spark = SparkSession.builder().getOrCreate()

    val applicationContextFileName = "/home/user0001/Desktop/Birkbeck/Project/1.0.0/configuration/applicationContext"
    ApplicationContext.useConfigFile(new File(applicationContextFileName))

    val springApplicationContextFileName = ApplicationContext.getContext.getString("springFramework.applicationContextFileName")

    // Generate the application context
    val ctx = new GenericApplicationContext();
    val xmlReader = new XmlBeanDefinitionReader(ctx);
    xmlReader.loadBeanDefinitions(new UrlResource(new URL("file", "", springApplicationContextFileName)));
    ctx.refresh();

    ApplicationContext.useConfigFile(new File(applicationContextFileName))

    // Get the Spark Context
    val sc = spark.sparkContext
    ApplicationContext.sc(sc)

    // Get an instance of a value predictor
    val predictorBeanName = "valuePredictor"
    val predictor = ctx.getBean(predictorBeanName).asInstanceOf[ValueGenerator]

    // Use parameter to evaluate a portolio at a given date
    val portfolioName = "Test_Portfolio_1"
    val valueAtDate = LocalDate.of(2016, 6, 2)

    // Get an instance of a prediction persistor
    val prediction = predictor.value(portfolioName, valueAtDate)

    // Write percentile values
    val instanceBeanName = "predictionPersistor"
    val writer = ctx.getBean(instanceBeanName).asInstanceOf[PredictionPersistor]

    // 
    val predictionRange = prediction.map(p => p._1)

    val hValue = ApplicationContext.getContext.getLong("mcs.mcsNumIterations")
    val percentile95 = getPercentile(95, predictionRange)
    writer.persist(portfolioName, valueAtDate, predictor.getClass.getSimpleName, hValue, 95, percentile95)

    val percentile99 = getPercentile(99, predictionRange)
    writer.persist(portfolioName, valueAtDate, predictor.getClass.getSimpleName, hValue, 99, percentile99)

  }
  private def getPercentile(percentile: Double, range: Array[Double]): Double = {

    // TODO: check values
    val sorted = range.sortWith(_ < _) // Sort ascending
    val index = (sorted.length / 100) * (100 - percentile).toInt
    sorted(index)
  }

}