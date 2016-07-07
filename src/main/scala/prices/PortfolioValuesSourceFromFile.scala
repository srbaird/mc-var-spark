package main.scala.prices

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import main.scala.application.ApplicationContext
import main.scala.portfolios.PortfolioValuesSource
import main.scala.transform.Transformable
import org.apache.spark.ml.Transformer
import java.time.LocalDate

class PortfolioValuesSourceFromFile(sc: SparkContext) extends PortfolioValuesSource[DataFrame] with Transformable {

  val appContext = ApplicationContext.getContext
  //
  // Context variables to locate the data
  //
  lazy val hdfsLocation = appContext.getString("fs.default.name")
  lazy val fileLocation = appContext.getString("portfolioHolding.fileLocation")
  lazy val portfolioFileType = appContext.getString("portfolioHolding.priceFileType")
  lazy val keyColumn = appContext.getString("portfolioHolding.keyColumn")
  lazy val valueColumn = appContext.getString("portfolioHolding.valueColumn")
  //
  private val logger = Logger.getLogger(getClass)
  //
  // For the implementation of Transformable
  //
  private var transformers = Vector[Transformer]()

  override def getAvailableCodes(): Seq[String] = throw new UnsupportedOperationException("Not implemented")
  override def getHoldings(portfolioCode: String, at: LocalDate): DataFrame = throw new UnsupportedOperationException("Not implemented")

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