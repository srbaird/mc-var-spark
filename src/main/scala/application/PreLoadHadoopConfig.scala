package main.scala.application

import java.io.File

trait PreLoadHadoopConfig {
  
  val hadoopConfigFile = "resources/main/applicationContext"
  def load = ApplicationContext.useHadoopConfig(new File(hadoopConfigFile))
}