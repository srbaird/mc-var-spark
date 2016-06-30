package test.scala.factors

import org.apache.spark.LocalSparkContext
import org.apache.spark.sql.DataFrame
import org.junit.runner.RunWith
import org.scalatest.Finders
import org.scalatest.FunSuite
import org.scalatest.Suite
import org.scalatest.Suite
import org.scalatest.junit.JUnitRunner
import main.scala.factors.RiskFactorSource
import main.scala.factors.RiskFactorSourceFromFile
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.scalatest.mock.MockitoSugar
import java.io.PrintWriter
import java.io.File
import main.scala.application.ApplicationContext

class RiskFactorSourceFromFileTest extends FunSuite with LocalSparkContext { self: Suite =>

  var instance: RiskFactorSource[DataFrame] = _

  // TODO: Needs to be pulled to a superclass
  override def beforeAll(): Unit = {

    // Create he Spark Context for the test suite
    sc = new SparkContext("local[4]", "RiskFactorSourceFromFileTest", new SparkConf(false))

    // Create a temporary config file to specify the test data to use
    val hdfsLocation = "\"hdfs://localhost:54310\""
    val fileLocation = "\"/project/test/initial-testing/\""
    val factorsFileName = "\"factors.clean.csv\""

    val configFileContents = s"riskFactor{hdfsLocation = ${hdfsLocation}, fileLocation = ${fileLocation}, factorsFileName = ${factorsFileName} }"
    
    println(configFileContents)

    val configFile = writeTempFile(configFileContents)
    try {
      val result = ApplicationContext.useConfigFile(configFile)
    } finally {
      configFile.delete()
    }
  }

  override def afterEach = {}

  override def beforeEach() {

    instance = RiskFactorSourceFromFile(sc)
  }

  override def afterAll = resetSparkContext()

  test("test reading a negative number of rows") {

    intercept[IllegalArgumentException] {
      instance.head(-1)
    }
  }

  test("test reading zero rows") {

    intercept[IllegalArgumentException] {
      instance.head(0)
    }
  }

  test("test reading a single row") {

    val cd = System.getProperty("user.dir")
    println(s"Current directory is ${cd}")
    val result = instance.head(1)
    assert(result.count() == 1)
  }

  /**
   * Helper methods
   */
  private def writeTempFile(content: String): File = {

    val tFile = File.createTempFile("tempConfigFile", null)
    val pw = new PrintWriter(tFile)
    pw.write(content)
    pw.close()
    tFile
  }
}