package main.scala.predict

import java.time.LocalDate

import org.apache.spark.sql.DataFrame

import main.scala.application.ApplicationContext
import main.scala.portfolios.PortfolioValuesSource

/**
 * Implement ValuePredictor to return a portfolio value using Monte Carlo simulation with h-day covariance matrix
 */
class HDayMCSValuePredictor(p: PortfolioValuesSource[DataFrame], ) extends ValuePredictor {

  val appContext = ApplicationContext.getContext

  val sc = ApplicationContext.sc

  lazy val mcsNumIterations = appContext.getInt("mcs.mcsNumIterations")


  override def predict(pCode: String, at: LocalDate): Array[(Double, Array[(String, Double)])] = {

    if (pCode == null || pCode.isEmpty()) {
      throw new IllegalArgumentException(s"Invalid portfolio code supplied: ${pCode}")
    }

    if (at == null) {
      throw new IllegalArgumentException(s"Invalid valuation date supplied: ${at}")
    }

    // Get a list of instruments with positions at the given date
    val holdings = p.getHoldings(pCode, at)
    
    
    
    // For each instrument get the appropriate model and predict against the feature samples
    
    

    throw new UnsupportedOperationException("Not implemented")
  }

}