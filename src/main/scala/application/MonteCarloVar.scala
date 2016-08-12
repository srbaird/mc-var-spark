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

object MonteCarloVar extends ConfigFromHDFS with SpringContextFromHDFS {

  //
  private val logger = Logger.getLogger(getClass)

  def main(args: Array[String]) {

    // TODO: Implement logging correctly
    Logger.getLogger(classOf[RackResolver]).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Get the Spark Context
    val spark = SparkSession.builder().getOrCreate()
    val sc = spark.sparkContext
    ApplicationContext.sc(sc)

    // TODO: Identify and process passed arguments
    run(args)
  }

  private def run(args: Array[String]) = {

    if (args.length < 3) {
      throw new IllegalArgumentException(s"Expected 3 arguments, got ${args.length}")
    }

    // Load the Config from first argument
    ApplicationContext.useConfigFile(loadConfig(args(0)))
    val portfolioName = args(1)
    val valueAtDate = LocalDate.parse(args(2))

    logger.info(s"Perform h-day MCS VaR for '${portfolioName}' at ${valueAtDate} using context file: '${args(0)}'")

    // Load the DI framework context from HDFS
    val springApplicationContextFileName = ApplicationContext.getContext.getString("springFramework.applicationContextFileName")
    val ctx = loadContext(springApplicationContextFileName)

    // Get an instance of a value predictor
    val predictorBeanName = ApplicationContext.getContext.getString("springFramework.predictorBeanName")
    logger.debug(s"Predictor bean name is '${predictorBeanName}'")
    val predictor = ctx.getBean(predictorBeanName).asInstanceOf[ValueGenerator]

    val predictionStartTime = System.currentTimeMillis()
    // Get an instance of a prediction persistor
    val prediction = predictor.value(portfolioName, valueAtDate)
    val predictionEndTime = System.currentTimeMillis()

    val sortedPrediction = prediction.sortBy(f => f._1)
 
    // Write percentile values
    val persistorBeanName = ApplicationContext.getContext.getString("springFramework.persistorBeanName")
    logger.debug(s"Persistor bean name is '${persistorBeanName}'")
    val writer = ctx.getBean(persistorBeanName).asInstanceOf[PredictionPersistor]

    // 
    val predictionRange = prediction.map(p => p._1)

    val hValue = ApplicationContext.getContext.getLong("hDayVolatility.hDayValue")
    val percentile95 = getPercentile(95, predictionRange)
    logger.info(s"Write 95% probability value of ${percentile95}")
    writer.persist(portfolioName, valueAtDate, predictor.getClass.getSimpleName, hValue, 95, percentile95)

    val percentile99 = getPercentile(99, predictionRange)
    logger.info(s"Write 99% probability value of ${percentile99}")
    writer.persist(portfolioName, valueAtDate, predictor.getClass.getSimpleName, hValue, 99, percentile99)

    logger.info(s"Completed h-day MCS VaR run. Prediction took ${predictionEndTime - predictionStartTime}(ms)")
  }
  private def getPercentile(percentile: Double, range: Array[Double]): Double = {

    // TODO: check values
    val sorted = range.sortWith(_ < _) // Sort ascending
    val index = (sorted.length / 100) * (100 - percentile).toInt
    sorted(index)
  }

}