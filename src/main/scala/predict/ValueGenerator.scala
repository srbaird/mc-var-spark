package main.scala.predict

import java.time.LocalDate
/**
 * Define the requirements to produce valuations for a portfolio code at a point in time 
 */
trait ValueGenerator {
  
  /**
   * Return an array of portfolio values with the  contributing dataset code/ value pairs
   */
  def value (pCode:String, at: LocalDate):Array[(Double, Array[(String, Double)])]
  
}