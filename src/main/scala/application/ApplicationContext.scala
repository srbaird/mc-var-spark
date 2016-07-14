package main.scala.application

import java.io.File
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import java.io.FileNotFoundException
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext

/**
 *
 */
object ApplicationContext {

  private var _context: Config = _
  
  private var _sc:SparkContext = _
  
  def sc = _sc
  def sc(ctx:SparkContext) =  {
    
    if (ctx == null) throw new NullPointerException("Supplied context  was null")
    _sc = ctx
  }

  def getContext: Config = _context

  private lazy val _conf: Configuration = buildHadoopConfig

  def getHadoopConfig = _conf

  def useConfigFile(configFile: File): Config = {

    if (configFile == null) throw new NullPointerException("Supplied config file was null")

    if (!configFile.exists()) throw new FileNotFoundException(s"File does not exist: ${configFile.getName}")

    _context = ConfigFactory.parseFile(configFile); getContext
  }

  private def buildHadoopConfig: Configuration = {

    // Build a config from the Application context values
    val conf = new Configuration()

    // Base Hadoop configuration options
    val nameNodeDir = "dfs.namenode.name.dir"
    val dataNodeDir = "dfs.namenode.name.dir"
    val dfsReplication = "dfs.replication"
    val tempDir = "hadoop.tmp.dir"
    val defaultFSName = "fs.default.name"

    conf.set(nameNodeDir, _context.getString(nameNodeDir))
    conf.set(dataNodeDir, _context.getString(dataNodeDir))
    conf.set(dfsReplication, _context.getString(dfsReplication))
    conf.set(tempDir, _context.getString(tempDir))
    conf.set(defaultFSName, _context.getString(defaultFSName))

    conf

  }
}