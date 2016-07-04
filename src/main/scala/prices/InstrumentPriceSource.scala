package main.scala.prices

import java.time.LocalDate

/**
 * Provide a sequence of date-price pairs for a given instrument
 *
 */
trait InstrumentPriceSource[T] {

  /**
   * Supplies all available prices for a given instrument code
   */
  def getPrices(dsCode: String): T

  /**
   * Supplies all available prices for a given instrument code after a specified date
   */
  def getPrices(dsCode: String, from: LocalDate): T = getPrices(dsCode, from, null)
  
  /**
   * Supplies all available prices for a given instrument code between two dates
   */
  def getPrices(dsCode: String, from: LocalDate, to: LocalDate): T
  
  /**
   * Provide a list of all available instrument codes
   */
  def getAvailableCodes():Seq[String]

}