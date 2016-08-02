package main.scala.predict

import main.scala.portfolios.PortfolioValuesSource
import java.time.LocalDate
import main.scala.prices.InstrumentPriceSource
import org.apache.spark.sql.DataFrame

class ObservationValueGenerator (pv: PortfolioValuesSource[DataFrame], pr: InstrumentPriceSource[DataFrame]) extends ValueGenerator {
  
  
    /**
   *
   */
  override def value(pCode: String, at: LocalDate): Array[(Double, Array[(String, Double)])] = {

    if (pCode == null || pCode.isEmpty()) {
      throw new IllegalArgumentException(s"Invalid portfolio code supplied: ${pCode}")
    }

    if (at == null) {
      throw new IllegalArgumentException(s"Invalid valuation date supplied: ${at}")
    }

        throw new UnsupportedOperationException("Not implemented")
    // Get a list of instruments with positions at the given date
//    val holdings = p.getHoldings(pCode, at)

    // If no holdings at this date then nothing more to do 
//    if (holdings.count() == 0) {
//      return Array[(Double, Array[(String, Double)])]() // 
//    }


   
  }
}