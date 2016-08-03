package main.scala.application

import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader
import org.springframework.context.support.GenericApplicationContext
import org.springframework.core.io.UrlResource
import java.net.URL
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.File
import main.scala.models.InstrumentModelGenerator
import java.time.LocalDate
import org.apache.spark.sql.SparkSession

object GenerateModels {

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

    // Get the Spark Context
    val sc = spark.sparkContext
    ApplicationContext.sc(sc)

    // Get an instance of a model generator
    val generatorBeanName = ApplicationContext.getContext.getString("springFramework.instrumentModelGeneratorBeanName")
    val generator = ctx.getBean(generatorBeanName).asInstanceOf[InstrumentModelGenerator]

    // Build parameters
    val modelsDSCode = "WIKI_CMC"
    val fromDate = LocalDate.of(2015, 6, 1)
    val toDate = LocalDate.of(2016, 5, 31)
    val result = generator.buildModel(fromDate, toDate, modelsDSCode)
  }
}