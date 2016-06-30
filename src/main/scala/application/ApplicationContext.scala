package main.scala.application

import java.io.File
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import java.io.FileNotFoundException

/**
 *
 */
object ApplicationContext {

  private var _context: Config = _

  def getContext: Config = _context

  def useConfigFile(configFile: File): Config = {

    if (configFile == null) throw new NullPointerException("Supplied config file was null")

    if (!configFile.exists()) throw new FileNotFoundException(s"File does not exist: ${configFile.getName}")

    _context = ConfigFactory.parseFile(configFile); getContext
  }

  def getHDSFLocation = "192.168.0.100:54310"
}