package test.scala.application

import org.scalatest.FunSuite
import main.scala.application.ApplicationContext
import java.io.File
import java.io.PrintWriter
import java.io.FileNotFoundException
import com.typesafe.config.ConfigException

/**
 * Tests for ApplicationContext.
 */
class ApplicationContextTestx extends FunSuite {

  /**
   * A null context will be returned if the config file is not set
   */
  test("get context without setting config file") {

    assert(ApplicationContext.getContext == null)
  }

  /**
   * Setting a null config file results in an exception
   */
  test("set null config file") {

    intercept[NullPointerException] {
      ApplicationContext.useConfigFile(null)
    }
  }

  /**
   * Setting a non-existent config file results in an exception
   */
  test("set non-existent config file") {

    val tFile = writeTempFile("")
    tFile.delete()
    intercept[FileNotFoundException] {
      ApplicationContext.useConfigFile(tFile)
    }
  }

  /**
   * Ensure that a valid property can be read
   */
  test("test valid property in temp file") {

    val key = "riskFactor.hdfsLocation"
    val value = "some hdfs location here"
    val configFile = writeTempFile(s"riskFactor{hdfsLocation = ${value}}")
    try {
      val result = ApplicationContext.useConfigFile(configFile).getString(key)
      assert(value == result)
    } finally {
      configFile.delete()
    }
  }

  /**
   * Reading an invalid property will result in an exception
   */
  test("test invalid property in temp file") {

    val key = "riskFactor.hdfsLocation"
    val configFile = writeTempFile("")
    try {
      intercept[ConfigException] {
        ApplicationContext.useConfigFile(configFile).getString(key)
      }
    } finally {
      configFile.delete()
    }
  }

  /**
   * Helper method to generate a temporary config file
   */
  private def writeTempFile(content: String): File = {

    val tFile = File.createTempFile("tempConfigFile", null)
    val pw = new PrintWriter(tFile)
    pw.write(content)
    pw.close()
    tFile
  }

}