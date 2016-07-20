package main.scala.predict

import java.time.LocalDate
import org.apache.spark.sql.DataFrame
import main.scala.application.ApplicationContext
import main.scala.portfolios.PortfolioValuesSource
import main.scala.factors.RiskFactorSource
import main.scala.util.Functions._

/**
 * Implement ValuePredictor to return a portfolio value using Monte Carlo simulation with h-day covariance matrix
 */
class HDayMCSValuePredictor(p: PortfolioValuesSource[DataFrame], f: RiskFactorSource[DataFrame],  ) extends ValuePredictor {

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
    
    // If no holdings at this date then nothing more to do 
    if (holdings.count() == 0) {
      return Array[(Double, Array[(String, Double)])]()  // 
    }
    
    // Generate a DataFrame of n samples 
    // For correlation purposes use 1 year of factor data
    val correlationFactors = f.factors(at.minusYears(1))
    val correlationFactorsAsMatrix = dfToArrayMatrix(correlationFactors)
    // 
    
    // For each instrument get the appropriate model and predict against the feature samples
    
    

    throw new UnsupportedOperationException("Not implemented")
  }

}