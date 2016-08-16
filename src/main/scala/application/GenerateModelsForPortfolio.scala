package main.scala.application

import java.time.LocalDate
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.yarn.util.RackResolver
import main.scala.models.InstrumentModelGenerator
import org.apache.log4j.Level
import org.apache.log4j.Logger
import main.scala.portfolios.PortfolioValuesSourceFromFile

object GenerateModelsForPortfolio extends ConfigFromHDFS with SpringContextFromHDFS {

  //
  private val logger = Logger.getLogger(getClass)

  def main(args: Array[String]) {

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
    val portfolioName = args(1)
    val fromDate = LocalDate.parse(args(2))
    val toDate = LocalDate.parse(args(3))

    logger.info(s"Generate models for '${portfolioName}' between ${fromDate} and ${toDate} using context file: '${args(0)}'")

    // Load the DI framework context from HDFS
    val springApplicationContextFileName = ApplicationContext.getContext.getString("springFramework.applicationContextFileName")
    val ctx = loadContext(springApplicationContextFileName)

    // Get an instance of a model generator
    val generatorBeanName = ApplicationContext.getContext.getString("springFramework.instrumentModelGeneratorBeanName")
    logger.debug(s"Model Generator bean name is '${generatorBeanName}'")
    val generator = ctx.getBean(generatorBeanName).asInstanceOf[InstrumentModelGenerator]

    // Get an instance of a portfolio source
    val portfoliosBeanName = "defaultPortfolioValuesSourceFromFile"
    val portfolioData = ctx.getBean(portfoliosBeanName).asInstanceOf[PortfolioValuesSourceFromFile]

    val instrumentColumn = ApplicationContext.getContext.getString("portfolioHolding.instrumentColumn")
    val dsCodes = portfolioData.getHoldings(portfolioName, toDate).collect().map(r => r.getAs[String](instrumentColumn))
    for (dsCode <- dsCodes) {
      generator.buildModel(fromDate, toDate, dsCode)
    }
//    portfolioData.getHoldings(portfolioName, toDate).collect().map(r => r.getAs[String](instrumentColumn)).foreach { dsCode => generator.buildModel(fromDate, toDate, dsCode) }


    logger.info("Generate models for portfolio complete")
  }
}