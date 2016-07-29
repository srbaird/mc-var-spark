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

object GenerateModels {

  def main(args: Array[String]) {

    // TODO: Implement logging correctly
    Logger.getLogger(classOf[RackResolver]).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // TODO: Identify and process passed arguments
    run
    println("Completed run")
  }

  private def run = {

    val springContextFileName = "/home/user0001/Desktop/Birkbeck/Project/1.0.0/configuration/application-context.xml"

    // Generate the application context
    val ctx = new GenericApplicationContext();
    val xmlReader = new XmlBeanDefinitionReader(ctx);
    xmlReader.loadBeanDefinitions(new UrlResource(new URL("file", "", springContextFileName)));
    ctx.refresh();

    val applicationContextFileNameBeanName = "applicationContextFileName"
    val applicationContextFileName = ctx.getBean(applicationContextFileNameBeanName).asInstanceOf[String]

    ApplicationContext.useConfigFile(new File(applicationContextFileName))

    // Generate the Spark Context
    val sc = new SparkContext("local[4]", "MonteCarloVaR", new SparkConf(false))
    ApplicationContext.sc(sc)

    // Get an instance of a value predictor
    val generatorBeanName = "instrumentModelGenerator"
    val generator = ctx.getBean(generatorBeanName).asInstanceOf[InstrumentModelGenerator]

    // Build parameters
    val modelsDSCode = "WIKI_CMC"
    val fromDate = LocalDate.of(2015, 6, 1)
    val toDate = LocalDate.of(2016, 5, 31)
    val result = generator.buildModel(fromDate, toDate, modelsDSCode)

    sc.stop()
  }
}