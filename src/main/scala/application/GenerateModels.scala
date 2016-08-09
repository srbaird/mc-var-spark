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

object GenerateModels extends ConfigFromHDFS with SpringContextFromHDFS {

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

    if (args.length < 4) {
      throw new IllegalArgumentException(s"Expected 4 arguments, got ${args.length}")
    }

    // Load the Config from first argument
    ApplicationContext.useConfigFile(loadConfig(args(0)))
    // Build parameters
    val modelsDSCode = args(1)
    val fromDate = LocalDate.parse(args(2))
    val toDate = LocalDate.parse(args(3))

    logger.info(s"Generate model for '${modelsDSCode}' between ${fromDate} and ${toDate} using context file: '${args(0)}'")

    // Load the DI framework context from HDFS
    val springApplicationContextFileName = ApplicationContext.getContext.getString("springFramework.applicationContextFileName")
    val ctx = loadContext(springApplicationContextFileName)

    // Get an instance of a model generator
    val generatorBeanName = ApplicationContext.getContext.getString("springFramework.instrumentModelGeneratorBeanName")
    logger.debug(s"Model Generator bean name is '${generatorBeanName}'")
    val generator = ctx.getBean(generatorBeanName).asInstanceOf[InstrumentModelGenerator]

    val result = generator.buildModel(fromDate, toDate, modelsDSCode)

    val successMsg = if(result(modelsDSCode)._1) "successful" else "NOT successful"
    logger.info("Generate model was ${successMsg}")
  }
}