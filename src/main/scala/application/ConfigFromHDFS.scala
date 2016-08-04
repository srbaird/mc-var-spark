package main.scala.application

import com.typesafe.config.ConfigFactory
import breeze.macros.expand.args
import org.apache.hadoop.fs.FileSystem
import java.io.InputStreamReader
import com.typesafe.config.Config
import breeze.macros.expand.args
import org.apache.hadoop.fs.Path
import breeze.macros.expand.args

trait ConfigFromHDFS {

  def loadConfig(location: String): Config = {

    val fs = FileSystem.get(ApplicationContext.getHadoopConfig)
    try {

      val reader = new InputStreamReader(fs.open(new Path(location)))
      ConfigFactory.parseReader(reader)
    } catch {
      case _: Throwable => throw new IllegalArgumentException(s"Cannot load Config from ${location}")
    }

  }
}