package main.scala.models

import java.io.FileNotFoundException
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.ml.util.MLWritable
import main.scala.application.ApplicationContext
import org.apache.spark.ml.Model
import org.apache.spark.ml.tuning.CrossValidatorModel

/**
 * Persistence layer using HDFS file system
 */
class InstrumentModelSourceFromFile(sc: SparkContext) extends InstrumentModelSource[Model[_]] {

  val appContext = ApplicationContext.getContext

  // Locate data
  val hdfsLocation = appContext.getString("fs.default.name")
  val modelsLocation = appContext.getString("instrumentModel.modelsLocation")
  val modelSchemasLocation = appContext.getString("instrumentModel.modelSchemasLocation")
  //
  private val logger = Logger.getLogger(this.getClass)

  override def getAvailableModels: Seq[String] = {

    // Use the Hadoop configuration from the Application Context rather than the Spark default
    val fs = FileSystem.get(ApplicationContext.getHadoopConfig)

    val p = new Path(modelsLocation)

    val files = fs.listLocatedStatus(p)
    var found = Array[String]()

    while (files.hasNext()) {

      val f = files.next().getPath.getName
      val dsCode = FilenameUtils.removeExtension(f)
      if (isLoadable(dsCode, fs)) {
        found = found :+ dsCode
      }
    }
    found
  }

  override def getModel(dsCode: String): Option[Model[_]] = {

    if (dsCode == null || dsCode.isEmpty) {
      throw new IllegalArgumentException(s"An invalid dataset code date was supplied: ${dsCode}")
    }

    // Use the Hadoop configuration from the Application Context rather than the Spark default
    val fs = FileSystem.get(ApplicationContext.getHadoopConfig)

    if (isLoadable(dsCode, fs)) {

      Option(loadModel(dsCode))

    } else {

      None
    }
  }

  // TODO: replace this with a Factory implementation
  private def loadModel(dsCode: String): CrossValidatorModel = {

    // To use the built-in loading facility (MLWritable) the path needs the HDFS file system prepended
    val modelPath = s"${hdfsLocation}${modelsLocation}${dsCode}"
    CrossValidatorModel.load(modelPath)
  }

  private def isLoadable(dsCode: String, fs: FileSystem): Boolean = {

    val modelPath = s"${modelsLocation}${dsCode}"
    val schemalPath = s"${modelSchemasLocation}${dsCode}"

    try {
      val m = fs.getFileStatus(new Path(modelPath))
      val s = fs.getFileStatus(new Path(schemalPath))
      m.isDirectory() // Basic validation to ensure that the model is a directory
    } catch {
      case _: Throwable => false
    }
  }
}