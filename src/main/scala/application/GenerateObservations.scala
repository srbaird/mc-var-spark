package main.scala.application

import java.io.File
import java.net.URL
import java.time.LocalDate

import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader
import org.springframework.context.support.GenericApplicationContext
import org.springframework.core.io.UrlResource

import main.scala.models.InstrumentModelGenerator
import main.scala.predict.PredictionPersistor
import main.scala.predict.ValueGenerator

object GenerateObservations extends ConfigFromHDFS with SpringContextFromHDFS {

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

    logger.info(s"Perform Observation generation for '${portfolioName}' at ${valueAtDate} using context file: '${args(0)}'")

    // Load the DI framework context from HDFS
    val springApplicationContextFileName = ApplicationContext.getContext.getString("springFramework.applicationContextFileName")
    val ctx = loadContext(springApplicationContextFileName)

    // Get an instance of a observation generator
    val generatorBeanName = ApplicationContext.getContext.getString("springFramework.observationGeneratorBeanName")
    logger.debug(s"Generator bean name is '${generatorBeanName}'")
    val generator = ctx.getBean(generatorBeanName).asInstanceOf[ValueGenerator]

    // Use parameter to evaluate a portolio at a given date

    // Get an instance of a prediction persistor
    val observation = generator.value(portfolioName, valueAtDate)

    // Write percentile values
    val persistorBeanName = ApplicationContext.getContext.getString("springFramework.persistorBeanName")
    logger.debug(s"Persistor bean name is '${persistorBeanName}'")
    val writer = ctx.getBean(persistorBeanName).asInstanceOf[PredictionPersistor]

    if (observation.size < 1) {
      logger.info("No observations were generated")
    } else {

      // Result is a single row
      val result = observation(0)._1

      val hValue = ApplicationContext.getContext.getLong("hDayVolatility.hDayValue")
      logger.info(s"Write observation value of ${hValue}")
      writer.persist(portfolioName, valueAtDate, generator.getClass.getSimpleName, hValue, 0, result)
    }
    logger.info("Completed observation generation")
  }
}