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

object CovarianceVar extends StandardArguments {

  def main(args: Array[String]) {

    // TODO: Implement logging correctly
    Logger.getLogger(classOf[RackResolver]).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    println(s"Invoked ${getClass.getSimpleName} with '${args.mkString(", ")}'")

    // TODO: Identify and process passed arguments
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

    // Get an instance of a value predictor
    val predictorBeanName = ApplicationContext.getContext.getString("springFramework.covariancePredictorBeanName")
    val predictor = ctx.getBean(predictorBeanName).asInstanceOf[ValueGenerator]

    // Use parameters to evaluate a portolio at a given date
    val portfolioName = validArgs._2
    val valueAtDate = validArgs._3

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