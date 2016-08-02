package main.scala.application

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.hadoop.yarn.util.RackResolver
import main.scala.predict.PredictionPersistor
import org.springframework.context.support.GenericApplicationContext
import java.time.LocalDate
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader
import main.scala.predict.ValuePredictor
import org.apache.spark.sql.SparkSession
import java.io.File
import org.springframework.core.io.UrlResource
import java.net.URL

object CovarianceVar {

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
    val predictorBeanName = "covarianceValuePredictor"
    val predictor = ctx.getBean(predictorBeanName).asInstanceOf[ValuePredictor]

    // Use parameter to evaluate a portolio at a given date
    val portfolioName = "Test_Portfolio_1"
    val valueAtDate = LocalDate.of(2016, 6, 2)

    // Get an instance of a prediction persistor
    val prediction = predictor.predict(portfolioName, valueAtDate)

    // Write percentile values
    val instanceBeanName = "predictionPersistor"
    val writer = ctx.getBean(instanceBeanName).asInstanceOf[PredictionPersistor]

    // Result is a single row
    val result = prediction(0)._1

    val hValue = ApplicationContext.getContext.getLong("mcs.mcsNumIterations")
    val sigma95 = ApplicationContext.getContext.getDouble("prediction.sigma95")
    val sigma99 = ApplicationContext.getContext.getDouble("prediction.sigma99")

    writer.persist(portfolioName, valueAtDate, predictor.getClass.getSimpleName, hValue, 95, sigma95 * result)

    writer.persist(portfolioName, valueAtDate, predictor.getClass.getSimpleName, hValue, 99, sigma99 * result)
  }
}