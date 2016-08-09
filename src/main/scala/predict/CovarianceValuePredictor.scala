package main.scala.predict

import main.scala.portfolios.PortfolioValuesSource
import main.scala.prices.InstrumentPriceSource
import java.time.LocalDate
import org.apache.spark.sql.DataFrame
import main.scala.util.Functions.dfToArrayMatrix
import main.scala.util.Functions.toDouble
import main.scala.application.ApplicationContext
import main.scala.transform.DoublesOnlyTransformer
import main.scala.transform.HDayVolatilityTransformer
import org.apache.commons.math3.linear.Array2DRowRealMatrix
import org.apache.commons.math3.stat.correlation.Covariance
import org.apache.log4j.Logger

class CovarianceValuePredictor(pv: PortfolioValuesSource[DataFrame], pr: InstrumentPriceSource[DataFrame]) extends ValueGenerator {

  val appContext = ApplicationContext.getContext
  //
  //
  //
  private val logger = Logger.getLogger(getClass)

  val sc = ApplicationContext.sc
  lazy val instrumentColumn = appContext.getString("portfolioHolding.instrumentColumn")
  lazy val valueColumn = appContext.getString("portfolioHolding.valueColumn")
  lazy val keyColumn = appContext.getString("instrumentPrice.keyColumn")
  lazy val priceColumn = appContext.getString("instrumentPrice.valueColumn")
  /**
   * Simple cash portfolio valuation. Given matrix M of asset returns over a time period and vector V of positions then
   * the calculation is the square root of (transpose(V) * M * V)
   */
  override def value(pCode: String, at: LocalDate): Array[(Double, Array[(String, Double)])] = {

    logger.debug(s"Generate a covariance value for '${pCode}' at ${at}")
    if (pCode == null || pCode.isEmpty()) {
      throw new IllegalArgumentException(s"Invalid portfolio code supplied: ${pCode}")
    }

    if (at == null) {
      throw new IllegalArgumentException(s"Invalid valuation date supplied: ${at}")
    }

    // Get a list of instruments with positions at the given date
    logger.trace(s"Get the holdings for the portfolio")
    val holdings = pv.getHoldings(pCode, at)

    // If no holdings at this date then nothing more to do 
    if (holdings.count() == 0) {
      logger.trace(s"No holdings, return an empty array")
      return Array[(Double, Array[(String, Double)])]() // 
    }

    // TODO: implement the portfolio holdings as an array rather than a DataFrame
    val holdingsAsArray = holdings.select(instrumentColumn, valueColumn).collect().map { x => (x.getString(0), x.getInt(1)) }

    // If no prices exist for any of the instruments then throw an exception
    logger.trace(s"Get the available price dataset codes")
    val availablePrices = pr.getAvailableCodes

    val missingPrices = holdingsAsArray.map(t => t._1).filter { dsCode => !availablePrices.contains(dsCode) }
    if (missingPrices.length > 0) {
      throw new IllegalStateException(s"No model for instruments: ${missingPrices.mkString(", ")}")
    }

    // Combine the price dataframes
    // For correlation purposes select 1 year each of price data
    // Don't include observations on the supplied date
    val to = at.minusDays(1)
    val from = to.minusYears(1)

    // TODO: Add method to trait to return an array of price values within a date range
    // Joining dataframes is only viable with a small number of instruments
    logger.trace(s"Combine the prices for ${} dataset codes between ${from} and ${to}")
    val pricesDF = if (holdingsAsArray.length == 1) {
      pr.getPrices(holdingsAsArray.head._1, from, to)
    } else {
      // 
      holdingsAsArray.tail.foldLeft(pr.getPrices(holdingsAsArray.head._1, from, to).withColumnRenamed(priceColumn, holdingsAsArray.head._1)) { (acc: DataFrame, e: (String, Int)) => acc.join(pr.getPrices(e._1, from, to).withColumnRenamed(priceColumn, e._1), keyColumn) }
    }

    // Turn the resolved price dataframe into an h-day variance matrix
    val variancePricesDF = new HDayVolatilityTransformer().transform(
      new DoublesOnlyTransformer().transform(
        pricesDF))

    // This represents the matrix M
    val variancePricesAsMatrix = new Array2DRowRealMatrix(dfToArrayMatrix(variancePricesDF))
    val m = new Covariance(variancePricesAsMatrix).getCovarianceMatrix

    // This represents the vector V as a 1xn matrix
    val portfolioWeights = holdingsAsArray.map(h => Array(toDouble(h._2)))
    assert(portfolioWeights.length == holdingsAsArray.length)
    val v = new Array2DRowRealMatrix(portfolioWeights)

    // Transform the weights and correlations into a single 1x1 array
    logger.trace(s"Transpose the weights and corelations")
    val r = v.transpose().multiply(m).multiply(v)
    assert(r.getColumnDimension == 1)
    assert(r.getRowDimension == 1)

    val portfolioReturn = Math.sqrt(r.getColumn(0)(0))

    // return with an empty array of individual instrument valuations
    logger.trace(s"Generated a covariance value of ${portfolioReturn}")
    Array((portfolioReturn, Array[(String, Double)]()))
  }

}