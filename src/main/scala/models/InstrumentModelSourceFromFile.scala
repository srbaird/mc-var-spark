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
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import java.util.Properties
import java.util.HashMap
import org.apache.hadoop.fs.FSDataOutputStream
import java.io.InputStreamReader
import com.typesafe.config.ConfigRenderOptions
import org.apache.spark.ml.regression.LinearRegressionModel
import main.scala.application.StringBeanFactory
import org.apache.spark.ml.util.MLReadable

/**
 * Persistence layer using HDFS file system
 */
class InstrumentModelSourceFromFile(f: StringBeanFactory[MLReadable[_]]) extends InstrumentModelSource[Model[_]] {

  private val appContext = ApplicationContext.getContext

  val sc = ApplicationContext.sc

  // Locate data
  lazy val modelsLocation = appContext.getString("instrumentModel.modelsLocation")
  lazy val modelSchemasLocation = appContext.getString("instrumentModel.modelSchemasLocation")
  //
  private val metadataClassName = "metadataClassName"
  //
  private val logger = Logger.getLogger(this.getClass)

  override def getAvailableModels: Seq[String] = {

    logger.debug(s"Get available model dataset codes")

    // Use the Hadoop configuration from the Application Context rather than the Spark default
    val fs = FileSystem.get(ApplicationContext.getHadoopConfig)
    val p = new Path(modelsLocation)

    val files = fs.listLocatedStatus(p)
    var found = Array[String]()

    while (files.hasNext()) {

      val f = files.next().getPath.getName
      val dsCode = FilenameUtils.removeExtension(f)
      logger.debug(s"Validate model for dataset code '${dsCode}'")
      if (isLoadable(dsCode, fs)) {
        found = found :+ dsCode
      }
    }
    found
  }

  override def getModel(dsCode: String): Option[Model[_]] = {

    logger.debug(s"Get model for '${dsCode}'")
    validateDSCode(dsCode)

    // Use the Hadoop configuration from the Application Context rather than the Spark default
    val fs = FileSystem.get(ApplicationContext.getHadoopConfig)

    if (isLoadable(dsCode, fs)) {

      // Use the metadata to generate the correct model type
      val es = readMetadata(dsCode, fs).entrySet().iterator()
      while (es.hasNext()) {
        val n = es.next()
        println(s"Entry ${n.getKey}: ${n.getValue}")
      }
      
      val metaDataClass = readMetadata(dsCode, fs).getString(metadataClassName)
      val model = loadModel(dsCode, metaDataClass)

      Option[Model[_]](loadModel(dsCode, metaDataClass))

    } else {

      None
    }
  }

  override def putModel(dsCode: String, model: Model[_]): Unit = {

    logger.debug(s"Put ${model} as model for '${dsCode}'")
    validateDSCode(dsCode)

    if (model == null) {
      throw new IllegalArgumentException(s"A null model was supplied")
    }

    // Use the Hadoop configuration from the Application Context rather than the Spark default
    val fs = FileSystem.get(ApplicationContext.getHadoopConfig)

    try {
      writeMetadata(dsCode, model, fs)
      try {
        (model.asInstanceOf[MLWritable]).write.overwrite().save(createModelPath(dsCode))
      } catch {
        case otherException: Throwable => {

          deleteMetadata(dsCode, fs)
          throw otherException
        }
      }
    } catch {
      case otherException: Throwable => throw otherException
    }
    // Trap any missed exceptions
    assert(isLoadable(dsCode, fs))
  }

  override def removeModel(dsCode: String): Unit = {

    logger.debug(s"Remove the model for '${dsCode}'")
    validateDSCode(dsCode)

    // Use the Hadoop configuration from the Application Context rather than the Spark default
    val fs = FileSystem.get(ApplicationContext.getHadoopConfig)

    try {
      deleteMetadata(dsCode, fs)
    } catch {
      case otherException: Throwable => // Defer exceptions
    }

    try {
      val modelPath = s"${modelsLocation}${dsCode}"
      fs.delete(new Path(modelPath), true) // Recursive delete as it is a directory
    } catch {
      case otherException: Throwable => // Defer exceptions
    }

    // Ensure the model has been removed
    assert(!isLoadable(dsCode, fs))
  }

  //
  // Dataset code validation
  //
  private def validateDSCode(dsCode: String): Unit = {

    if (dsCode == null || dsCode.isEmpty) {
      throw new IllegalArgumentException(s"An invalid dataset code date was supplied: ${dsCode}")
    }
  }

  //
  // Generate meta-data for the supplied Model
  //
  private def generateConfig(model: Model[_]): Config = {

    logger.trace(s"Generate a config from ${model}")
    val metadata = new HashMap[String, String]() // ConfigFactory requires a Java map
    metadata.put(metadataClassName, model.getClass.getSimpleName) // Simple implementation for the time being
    ConfigFactory.parseMap(metadata)
  }

  //
  // Write a meta-data file
  //
  private def writeMetadata(dsCode: String, model: Model[_], fs: FileSystem): Boolean = {

    logger.trace(s"Write the metadata from ${model} for '${dsCode}'")
    val os: FSDataOutputStream = try {

      fs.create(createMetadataPath(dsCode), true)
    } catch {
      case _: Throwable => return false
    }
    try {
      val fileContents = generateConfig(model).root().render(ConfigRenderOptions.concise())
      os.writeUTF(fileContents)
      true
    } catch {
      case _: Throwable => return false
    } finally {
      os.close()
    }
  }

  //
  // Read a meta-data file
  //
  private def readMetadata(dsCode: String, fs: FileSystem): Config = {

    logger.trace(s"Read the metadata for '${dsCode}'")
    val metaDataInputStream = fs.open(createMetadataPath(dsCode))
    ConfigFactory.parseString(metaDataInputStream.readUTF())
  }

  //
  // Remove the meta-data file
  //
  private def deleteMetadata(dsCode: String, fs: FileSystem): Unit = {

    logger.trace(s"Remove the metadata for '${dsCode}'")
    fs.delete(createMetadataPath(dsCode), false)
  }

  //
  // Create a metadata Path
  //
  private def createMetadataPath(dsCode: String): Path = {
    new Path(s"${modelSchemasLocation}${dsCode}")
  }

  //
  // Create a hdfs path to a the model
  //
  private def createModelPath(dsCode: String): String = {
    val hdfsLocation = ApplicationContext.getHadoopConfig.get("fs.default.name")
    s"${hdfsLocation}${modelsLocation}${dsCode}"
  }

  // TODO: replace this with a Factory implementation
  private def loadModel(dsCode: String, modelClass: String): Model[_] = {

    logger.trace(s"Load a ${modelClass} model for '${dsCode}'")
    f.create(modelClass).load(createModelPath(dsCode)).asInstanceOf[Model[_]]
    //   LinearRegressionModel.load(createModelPath(dsCode))
  }

  //
  //  Assert whether there exists both a model directory and a metadata file
  //
  private def isLoadable(dsCode: String, fs: FileSystem): Boolean = {

    val modelPath = s"${modelsLocation}${dsCode}"

    try {
      val m = fs.getFileStatus(new Path(modelPath))
      val s = fs.getFileStatus(createMetadataPath(dsCode))
      m.isDirectory() // Basic validation to ensure that the model is a directory
    } catch {
      case _: Throwable => false
    }
  }
}