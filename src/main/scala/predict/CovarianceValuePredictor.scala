package main.scala.predict

import main.scala.portfolios.PortfolioValuesSource
import main.scala.prices.InstrumentPriceSource
import java.time.LocalDate
import org.apache.spark.sql.DataFrame

class CovarianceValuePredictor(pv: PortfolioValuesSource[DataFrame], pr: InstrumentPriceSource[DataFrame]) extends ValuePredictor{
  
    override def predict(pCode: String, at: LocalDate): Array[(Double, Array[(String, Double)])] = {
      throw new UnsupportedOperationException("Not implemented")
    }
  
}