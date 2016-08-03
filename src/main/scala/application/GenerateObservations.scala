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

object GenerateObservations  extends StandardArguments{

  def main(args: Array[String]) {

    // TODO: Implement logging correctly
    Logger.getLogger(classOf[RackResolver]).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    println(s"Invoked ${getClass.getSimpleName} with '${args.mkString(", ")}'")

    run(args)
    println("Completed run")
  }

  private def run(args: Array[String]) = {

    val validArgs = validateArgs(args)

    val spark = SparkSession.builder().getOrCreate()

    ApplicationContext.useConfigFile(new File(validArgs._1))

    val springApplicationContextFileName = ApplicationContext.getContext.getString("springFramework.applicationContextFileName")

    // Generate the application context
    val ctx = new GenericApplicationContext();
    val xmlReader = new XmlBeanDefinitionReader(ctx);
    xmlReader.loadBeanDefinitions(new UrlResource(new URL("file", "", springApplicationContextFileName)));
    ctx.refresh();

    // Get the Spark Context
    val sc = spark.sparkContext
    ApplicationContext.sc(sc)

    // Get an instance of a model generator
    val generatorBeanName = ApplicationContext.getContext.getString("springFramework.observationGeneratorBeanName")
    val generator = ctx.getBean(generatorBeanName).asInstanceOf[ValueGenerator]

    // Use parameter to evaluate a portolio at a given date
    val portfolioName = validArgs._2
    val valueAtDate = validArgs._3

    // Get an instance of a prediction persistor
    val observation = generator.value(portfolioName, valueAtDate)

    // Write percentile values
    val instanceBeanName = ApplicationContext.getContext.getString("springFramework.persistorBeanName")
    val writer = ctx.getBean(instanceBeanName).asInstanceOf[PredictionPersistor]

    // Result is a single row
    val result = observation(0)._1

    val hValue = ApplicationContext.getContext.getLong("hDayVolatility.hDayValue")

    writer.persist(portfolioName, valueAtDate, generator.getClass.getSimpleName, hValue, 0, result)
  }
}