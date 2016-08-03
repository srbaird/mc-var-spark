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

  private var _sc: SparkContext = _

  def sc = _sc
  def sc(ctx: SparkContext) = {

    if (ctx == null) throw new NullPointerException("Supplied context  was null")
    _sc = ctx
  }

  def getContext: Config = _context

  private var _conf: Configuration = _

  def getHadoopConfig = {

    if (sc != null && sc.hadoopConfiguration.get("fs.default.name") != "file:///") {
      sc.hadoopConfiguration
    } else {
      _conf
    }
  }

  def useConfigFile(configFile: File): Config = {

    if (configFile == null) throw new NullPointerException("Supplied config file was null")

    if (!configFile.exists()) throw new FileNotFoundException(s"File does not exist: ${configFile.getName}")

    _context = ConfigFactory.parseFile(configFile); getContext
  }

  def useHadoopConfig(configFile: File) = {

    // Build a config from the Application context values
    val hadoopContext = ConfigFactory.parseFile(configFile);
    _conf = new Configuration()

    // Base Hadoop configuration options
    val nameNodeDir = "dfs.namenode.name.dir"
    val dataNodeDir = "dfs.namenode.name.dir"
    val dfsReplication = "dfs.replication"
    val tempDir = "hadoop.tmp.dir"
    val defaultFSName = "fs.default.name"

    _conf.set(nameNodeDir, hadoopContext.getString(nameNodeDir))
    _conf.set(dataNodeDir, hadoopContext.getString(dataNodeDir))
    _conf.set(dfsReplication, hadoopContext.getString(dfsReplication))
    _conf.set(tempDir, hadoopContext.getString(tempDir))
    _conf.set(defaultFSName, hadoopContext.getString(defaultFSName))
  }
}