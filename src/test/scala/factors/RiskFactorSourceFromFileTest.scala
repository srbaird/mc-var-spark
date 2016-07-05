package test.scala.factors

import java.io.File
import java.io.PrintWriter
import java.time.LocalDate

import org.apache.spark.LocalSparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.scalatest.Finders
import org.scalatest.FunSuite
import org.scalatest.Suite

import main.scala.application.ApplicationContext
import main.scala.factors.RiskFactorSourceFromFile
import main.scala.transform.ValueDateTransformer

/**
 * Test the file-backed RiskFactorSource -> DataFrame. The tests are predicated on a file containing a months worth of data
 */
class RiskFactorSourceFromFileTest extends FunSuite with LocalSparkContext { self: Suite =>

  var instance: RiskFactorSourceFromFile = _

  // Test file details. Some of the test conditions are linked to the contents
  val hdfsLocation = "\"hdfs://localhost:54310\""
  val fileLocation = "\"/project/test/initial-testing/\""
  val factorsFileName = "\"factors.clean.may2016.csv\""
  val testFileLength = 31L

  // TODO: Needs to be pulled to a superclass
  override def beforeAll(): Unit = {

    // Create the Spark Context for the test suite
    sc = new SparkContext("local[4]", "RiskFactorSourceFromFileTest", new SparkConf(false))

    // Create a temporary config file to specify the test data to use
    val configFileContents = s"riskFactor{hdfsLocation = ${hdfsLocation}, fileLocation = ${fileLocation}, factorsFileName = ${factorsFileName} }"
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
   instance.add(new ValueDateTransformer())
  }

  override def afterAll = resetSparkContext()

  /**
   *
   */
  test("test reading a negative number of rows") {

    intercept[IllegalArgumentException] {
      instance.head(-1)
    }
  }

  /**
   *
   */
  test("test reading zero rows") {

    intercept[IllegalArgumentException] {
      instance.head(0)
    }
  }
  /**
   * The test file is assumed to have at least one row in it. The assertion of content is dependent on the file
   */
  test("test reading a single row") {

    val result = instance.head(1)
    assert(result.count() == 1)
  }
  /**
   * The test file is assumed to have fewer than 100 rows
   */
  test("test reading 100 rows from smaller data set") {

    val result = instance.head(100)
    assert(result.count() == testFileLength) // Test file has only 31 rows
  }

  /**
   * Passing a null start date argument should result in an exception
   */
  test("test reading by date with a null start date") {

    intercept[IllegalArgumentException] {
      instance.factors(null)
    }
  }
  
  /**
   * Return all rows in the file
   */
  test("test reading all rows ") {
    val result = instance.factors()
    assert(result.count() == testFileLength) // Test file has only 31 rows
  }

  /**
   * Passing a from-date that is greater than the to-date should result in an exception
   */
  test("test reading by date where from-date is greater than to-date") {

    val day = 1
    val month = 1
    val year = 2016

    val toDate = LocalDate.of(year, month, day)
    val fromDate = LocalDate.of(year, month, day + 1)
    intercept[IllegalArgumentException] {
      instance.factors(fromDate, toDate)
    }
  }

  /**
   * The test file is assumed to contain all rows after 01-Jan-2016
   */
  test("test reading rows with a start date >= 01-Jan-2016") {

    val day = 1
    val month = 1
    val year = 2016
    val fromDate = LocalDate.of(year, month, day)
    val result = instance.factors(fromDate)
    assert(result.count() == testFileLength) // All 
  }

  /**
   * The test file is assumed to contain some rows after 15-May-2016
   */
  test("test reading rows with a start date >= 15-May-2016") {

    val expectedNumRows = 17L
    val day = 15
    val month = 5
    val year = 2016
    val fromDate = LocalDate.of(year, month, day)
    val result = instance.factors(fromDate)
    assert(result.count() == expectedNumRows)
  }

  /**
   * The test file is assumed to contain some rows between 01 and 02 May 2016
   */
  test("test reading rows with a between 01-May-2016 and 02-May-2016") {

    val expectedNumRows = 2L
    val day = 1
    val month = 5
    val year = 2016
    val fromDate = LocalDate.of(year, month, day)
    val toDate = LocalDate.of(year, month, day + 1)
    val result = instance.factors(fromDate, toDate)
    assert(result.count() == expectedNumRows)
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