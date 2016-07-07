package main.scala.prices

import java.time.LocalDate

import scala.Vector

import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.DataFrame

import main.scala.application.ApplicationContext
import main.scala.portfolios.PortfolioValuesSource
import main.scala.transform.Transformable

class PortfolioValuesSourceFromFile(sc: SparkContext) extends PortfolioValuesSource[DataFrame] with Transformable {

  val appContext = ApplicationContext.getContext
  //
  // Context variables to locate the data
  //
  lazy val hdfsLocation = appContext.getString("fs.default.name")
  lazy val fileLocation = appContext.getString("portfolioHolding.fileLocation")
  lazy val portfolioFileType = appContext.getString("portfolioHolding.portfolioFileType")
  lazy val keyColumn = appContext.getString("portfolioHolding.keyColumn")
  lazy val valueColumn = appContext.getString("portfolioHolding.valueColumn")
  //
  private val logger = Logger.getLogger(getClass)
  //
  // For the implementation of Transformable
  //
  private var transformers = Vector[Transformer]()

  override def getAvailableCodes(): Seq[String] = {

    // Use the Hadoop configuration from the Application Context rather than the Spark default
    val fs = FileSystem.get(ApplicationContext.getHadoopConfig)

    val p = new Path(fileLocation)
    val files = fs.listFiles(p, false)
    var found = Array[String]()

    while (files.hasNext()) {
      val f = files.next().getPath.getName
      if (f.endsWith(portfolioFileType)) {
        found = found :+ FilenameUtils.removeExtension(f)
      }
    }
    found
  }

  override def getHoldings(portfolioCode: String, at: LocalDate): DataFrame = {

    if (portfolioCode == null || portfolioCode.isEmpty()) {
      throw new IllegalArgumentException(s"An invalid portfolio code was supplied: ${portfolioCode}")
    }
    if (at == null) {
      throw new IllegalArgumentException(s"An invalid value at date was supplied: ${at}")
    }
    null
  }

  /**
   * Add a Transformer to the sequence
   */
  override def add(t: Transformer): Unit = {

    // don't allow a null value to be added
    if (t == null) {
      throw new IllegalArgumentException(s"Cannot add a null value")
    }
    transformers = transformers :+ t
  }

  /**
   * Apply available transformers in sequence
   */
  override def transform(d: DataFrame): DataFrame = {

    if (transformers.isEmpty) {
      d
    } else {
      transformers.foldLeft(d)((acc, t) => t.transform(acc))
    }
  }
}