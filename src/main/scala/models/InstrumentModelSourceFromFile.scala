package main.scala.models

import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import main.scala.application.ApplicationContext
import org.apache.spark.ml.util.MLWritable

/**
 * Persistence layer using HDFS file system
 */
class InstrumentModelSourceFromFile(sc: SparkContext) extends InstrumentModelSource[MLWritable] {

  val appContext = ApplicationContext.getContext

  // Locate data
  val hdfsLocation = appContext.getString("fs.default.name")
  val modelLocation = appContext.getString("instrumentModel.modelLocation")
  //
  private val logger = Logger.getLogger(this.getClass)

  override def getAvailableModels: Seq[String] = {
    // Use the Hadoop configuration from the Application Context rather than the Spark default
    val fs = FileSystem.get(ApplicationContext.getHadoopConfig)

    val p = new Path(modelLocation)

    val files = fs.listLocatedStatus(p)
    var found = Array[String]()

    while (files.hasNext()) {
      val f = files.next().getPath.getName

      found = found :+ FilenameUtils.removeExtension(f)
    }
    found
  }
  
  override def getModel(dsCode:String):Option[MLWritable] = {throw new UnsupportedOperationException("Not implemented")}
}