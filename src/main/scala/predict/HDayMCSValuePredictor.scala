package main.scala.predict

import java.time.LocalDate
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import main.scala.application.ApplicationContext
import main.scala.factors.RiskFactorSource
import main.scala.portfolios.PortfolioValuesSource
import main.scala.util.Functions._
import org.apache.spark.sql.Row
import main.scala.models.InstrumentModelSource
import org.apache.spark.ml.Model

/**
 * Implement ValuePredictor to return a portfolio value using Monte Carlo simulation with h-day covariance matrix
 */
class HDayMCSValuePredictor(p: PortfolioValuesSource[DataFrame], f: RiskFactorSource[DataFrame], c: CorrelatedSampleGenerator, m: InstrumentModelSource[Model[_]]) extends ValuePredictor {

  val appContext = ApplicationContext.getContext

  val sc = ApplicationContext.sc

  lazy val mcsNumIterations = appContext.getLong("mcs.mcsNumIterations")

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
      return Array[(Double, Array[(String, Double)])]() // 
    }

    // If no model exists for any of the instruments then throw an exception

    // Generate a DataFrame of n samples 
    // For correlation purposes use 1 year of factor data
    val correlationFactors = f.factors(at.minusYears(1))
    val correlationFactorsAsMatrix = dfToArrayMatrix(correlationFactors)
    // 
    val correlatedSamples = c.sampleCorrelated(mcsNumIterations, correlationFactorsAsMatrix)

    val sqlc = new SQLContext(sc)
    val correlatedSamplesAsRDDOfRows = sc.parallelize(correlatedSamples.map { a => Row.fromSeq(a) })
    val correlatedSamplesAsDF = sqlc.createDataFrame(correlatedSamplesAsRDDOfRows, correlationFactors.schema)

    // For each instrument get the appropriate model and predict against the feature samples
    // U

    throw new UnsupportedOperationException("Not implemented")
  }

}