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
import main.scala.transform.DoublesOnlyTransformer
import main.scala.transform.HDayVolatilityTransformer

/**
 * Implement ValuePredictor to return a portfolio value using Monte Carlo simulation with h-day covariance matrix
 */
class HDayMCSValuePredictor(p: PortfolioValuesSource[DataFrame], f: RiskFactorSource[DataFrame], c: CorrelatedSampleGenerator, m: InstrumentModelSource[Model[_]]) extends ValuePredictor {

  val appContext = ApplicationContext.getContext

  val sc = ApplicationContext.sc

  lazy val mcsNumIterations = appContext.getLong("mcs.mcsNumIterations")
  lazy val instrumentColumn = appContext.getString("portfolioHolding.instrumentColumn")
  lazy val valueColumn = appContext.getString("portfolioHolding.valueColumn")

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

    // TODO: implement the portfolio holdings as an array rather than a DataFrame
    val holdingsAsArray = holdings.select(instrumentColumn, valueColumn).collect().map { x => (x.getString(0), x.getInt(1)) }

    // If no model exists for any of the instruments then throw an exception
    val availableModels = m.getAvailableModels

    val missingModels = holdingsAsArray.map(t => t._1).filter { dsCode => !availableModels.contains(dsCode) }
    if (missingModels.length > 0) {
      throw new IllegalStateException(s"No model for instruments: ${missingModels.mkString(", ")}")
    }

    // Generate a DataFrame of n samples 
    // For correlation purposes use 1 year of factor data
    
    // TODO: remove this to a DI implementation
    val correlationFactors = new HDayVolatilityTransformer().transform(
        new DoublesOnlyTransformer().transform(
            f.factors(at.minusYears(1))))
    
    
    val correlationFactorsAsMatrix = dfToArrayMatrix(correlationFactors)
    // 
    val correlatedSamples = c.sampleCorrelated(mcsNumIterations, correlationFactorsAsMatrix)

    val sqlc = new SQLContext(sc)
    val correlatedSamplesAsRDDOfRows = sc.parallelize(correlatedSamples.map { a => Row.fromSeq(a) })
    val correlatedSamplesAsDF = sqlc.createDataFrame(correlatedSamplesAsRDDOfRows, correlationFactors.schema)

    // For each instrument get the appropriate model and predict against the feature samples
    // Use a for loop initially to restrict the parallelism to the samples dimension
    var returnArray = Array[(Double, Array[(String, Double)])]()
    for (portfolioHolding <- holdingsAsArray) {
      val dsCode = portfolioHolding._1
      val holding = portfolioHolding._2
      val predictions = dfToArrayMatrix(m.getModel(dsCode).get.transform(correlatedSamplesAsDF).select("prediction")) // TODO: add "prediction" to context file
      returnArray = predictions.map { p => (p(0) * holding, Array[(String, Double)]((dsCode.toString(), toDouble(p(0))))) }
    }
    returnArray
  }

}