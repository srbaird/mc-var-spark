package main.scala.portfolios

import java.time.LocalDate

/**
 * Return a sequence of individual instrument positions at a given data for
 * name portfolio grouping
 */
trait PortfolioValuesSource[T] {

  /**
   * Provide a list of all available portfolio codes
   */
  def getAvailableCodes(): Seq[String]

  /**
   * Return the holdings for the supplied portfolio at the supplied date
   */
   def getHoldings(portfolioCode: String, at: LocalDate): T
}