package test.scala.application

import java.io.File
import java.io.PrintWriter

import org.apache.hadoop.yarn.util.RackResolver
import org.apache.spark.LocalSparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import org.scalatest.Suite
import org.scalatest.Suite
import org.apache.log4j.Level
import org.apache.log4j.Logger

/**
 * Base for Spark Context based tests
 */
abstract class SparkTestBase extends FunSuite with LocalSparkContext { self: Suite =>

  // Base Hadoop configuration options
  val nameNodeDir = "\"file:/usr/local/hadoop_store/hdfs/namenode\""
  val dataNodeDir = "\"file:/usr/local/hadoop_store/hdfs/datanode\""
  val dfsReplication = "\"1\""
  val tempDir = "\"/app/hadoop/tmp\""
  val defaultFSName = "\"hdfs://localhost:54310\""

  // Application Context entry for Hadoop base configuration

  val hadoopAppContextEntry = s"""dfs {namenode {name {dir = ${nameNodeDir} }} , datanode {data {dir = ${dataNodeDir}}}, replication = ${dfsReplication}} 
                            ,hadoop {tmp {dir = ${tempDir}}}
                            ,fs {default {name = ${defaultFSName}}}"""

  override def beforeAll(): Unit = {

    Logger.getLogger(classOf[RackResolver]).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    // Create the Spark Context for the test suite
    sc = new SparkContext("local[4]", "RiskFactorSourceFromFileTest", new SparkConf(false))

  }
  
  override def afterAll = resetSparkContext


  /**
   * Helper methods
   */
  def writeTempFile(content: String): File = {

    val tFile = File.createTempFile("tempConfigFile", null)
    val pw = new PrintWriter(tFile)
    pw.write(content)
    pw.close()
    tFile
  }
}