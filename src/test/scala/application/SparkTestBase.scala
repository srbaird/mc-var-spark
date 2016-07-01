package test.scala.application

import org.apache.spark.LocalSparkContext
import org.scalatest.FunSuite
import org.scalatest.Suite
import org.scalatest.Suite
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.PrintWriter
import java.io.File

/**
 * Base for Spark Context based tests
 */
abstract class SparkTestBase extends FunSuite with LocalSparkContext { self: Suite =>

  override def beforeAll(): Unit = {

    // Create the Spark Context for the test suite
    sc = new SparkContext("local[4]", "RiskFactorSourceFromFileTest", new SparkConf(false))

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