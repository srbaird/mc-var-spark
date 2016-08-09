package main.scala.predict

import java.time.LocalDate
import org.apache.spark.sql.DataFrame
import main.scala.application.ApplicationContext
import main.scala.portfolios.PortfolioValuesSource
import main.scala.prices.InstrumentPriceSource
import main.scala.transform.DoublesOnlyTransformer
import main.scala.transform.HDayVolatilityTransformer
import main.scala.util.Functions._
import org.apache.spark.sql.functions.desc
import org.apache.log4j.Logger
/**
 * Implementation of ValueGenerator to provide a valuation of actual h-day variances for a portfolio at a given date
 */
class ObservationValueGenerator(pv: PortfolioValuesSource[DataFrame], pr: InstrumentPriceSource[DataFrame]) extends ValueGenerator {

  val appContext = ApplicationContext.getContext
  //
  //
  //
  private val logger = Logger.getLogger(getClass)

  val sc = ApplicationContext.sc
  lazy val instrumentColumn = appContext.getString("portfolioHolding.instrumentColumn")
  lazy val valueColumn = appContext.getString("portfolioHolding.valueColumn")
  lazy val priceValueColumn = appContext.getString("instrumentPrice.valueColumn")
  lazy val sortColumn = appContext.getString("instrumentPrice.keyColumn")
  /**
   *
   */
  override def value(pCode: String, at: LocalDate): Array[(Double, Array[(String, Double)])] = {

    logger.debug(s"Generate an observation value for '${pCode}' at ${at}")
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
    val availablePrices = pr.getAvailableCodes

    val missingPrices = holdingsAsArray.map(t => t._1).filter { dsCode => !availablePrices.contains(dsCode) }
    if (missingPrices.length > 0) {
      throw new IllegalStateException(s"No model for instruments: ${missingPrices.mkString(", ")}")
    }
    // Map each instrument holding to the price at the given date
    logger.trace(s"Generate valuations for ${holdingsAsArray.length} instruments")
    val valuations = holdingsAsArray.map(h => (h._1, getHDayPriceForInstrument(h._1, at) * h._2))

    Array((valuations.foldLeft(0D) { (acc, t) => acc + t._2 }, valuations))
  }

  //
  // 
  //
  private def getHDayPriceForInstrument(instrument: String, at: LocalDate): Double = {

    // Use a 20 days worth of h-day prices TODO: test that this has enough data for the h-day variable
    val hDayPrices = new HDayVolatilityTransformer().transform(
      new DoublesOnlyTransformer().transform(
        pr.getPrices(instrument, at.minusDays(20), at).select(priceValueColumn)))

    // The last value should be the h-day variance for the given date
    dfToArrayMatrix(hDayPrices).last(0)
  }
}