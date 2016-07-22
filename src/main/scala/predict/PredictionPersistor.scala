package main.scala.predict

import java.time.LocalDate

trait PredictionPersistor {
  
  /**
   * 
   */
  def persist(portfolioCode:String, at:LocalDate, hValue:Double, pValue:Double, valuation:Double)
  
}