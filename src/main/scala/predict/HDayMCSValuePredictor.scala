package main.scala.predict

import java.time.LocalDate

/**
 * Implement ValuePredictor to return a portfolio value using Monte Carlo simulation with h-day covariance matrix
 */
class HDayMCSValuePredictor extends ValuePredictor{
  
  override   def predict (pCode:String, at: LocalDate):Array[(Double, Array[(String, Double)])] = throw new UnsupportedOperationException("Not implemented")
  
}