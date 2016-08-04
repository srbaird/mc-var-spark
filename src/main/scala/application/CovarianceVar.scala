package main.scala.application

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.hadoop.yarn.util.RackResolver
import main.scala.predict.PredictionPersistor
import org.springframework.context.support.GenericApplicationContext
import java.time.LocalDate
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader
import org.apache.spark.sql.SparkSession
import java.io.File
import org.springframework.core.io.UrlResource
import java.net.URL
import main.scala.predict.ValueGenerator
import com.typesafe.config.ConfigFactory
import java.io.InputStreamReader
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.springframework.core.io.InputStreamResource

object CovarianceVar extends ConfigFromHDFS with SpringContextFromHDFS {

  def main(args: Array[String]) {

    // TODO: Implement logging correctly
    Logger.getLogger(classOf[RackResolver]).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    run(args)
    println("Completed run")
  }

  //
  // Expected arguments are: application context file name, portfolio code, at-date in YYYY-MM-DD format
  //
  private def run(args: Array[String]) = {

    if (args.length < 3) {
      throw new IllegalArgumentException(s"Expected 3 arguments, got ${args.length}")
    }

    // Load the Config from first argument
    ApplicationContext.useConfigFile(loadConfig(args(0)))

    // Load the DI framework context from HDFS
    val springApplicationContextFileName = ApplicationContext.getContext.getString("springFramework.applicationContextFileName")
    val ctx = loadContext(springApplicationContextFileName)

    // Get the Spark Context
    val spark = SparkSession.builder().getOrCreate()
    val sc = spark.sparkContext
    ApplicationContext.sc(sc)

    // Get an instance of a value predictor
    val predictorBeanName = ApplicationContext.getContext.getString("springFramework.covariancePredictorBeanName")
    val predictor = ctx.getBean(predictorBeanName).asInstanceOf[ValueGenerator]

    // Use parameters to evaluate a portolio at a given date
    val portfolioName = args(1)
    val valueAtDate = LocalDate.parse(args(2))

    // Get an instance of a prediction persistor
    val prediction = predictor.value(portfolioName, valueAtDate)

    // Write percentile values
    val instanceBeanName = ApplicationContext.getContext.getString("springFramework.persistorBeanName")
    val writer = ctx.getBean(instanceBeanName).asInstanceOf[PredictionPersistor]

    // Result is a single row
    val result = prediction(0)._1

    val hValue = ApplicationContext.getContext.getLong("hDayVolatility.hDayValue")
    val sigma95 = ApplicationContext.getContext.getDouble("predictions.sigma95")
    val sigma99 = ApplicationContext.getContext.getDouble("predictions.sigma99")

    writer.persist(portfolioName, valueAtDate, predictor.getClass.getSimpleName, hValue, 95, -1 * sigma95 * result)

    writer.persist(portfolioName, valueAtDate, predictor.getClass.getSimpleName, hValue, 99, -1 * sigma99 * result)
  }
}