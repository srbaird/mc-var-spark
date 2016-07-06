package main.scala.models

import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.Model
import org.apache.spark.sql.DataFrame

import main.scala.factors.RiskFactorSource
import main.scala.prices.InstrumentPriceSource

/**
 * For want of a better name, the default model generator for data sets
 */
class DefaultInstrumentModelGenerator(sc: SparkContext) extends InstrumentModelGenerator with InstrumentModelGeneratorSources[DataFrame,Model[_]] {

  // Source of Market Risk Factor data (features)
  private var factors: RiskFactorSource[DataFrame] = _

  // Source of Instrument prices data (label)
  private var prices: InstrumentPriceSource[DataFrame] = _

  // Destination of generated models
  private var models: InstrumentModelSource[Model[_]] = _

  /**
   *
   */
  override def riskFactorSource(source: RiskFactorSource[DataFrame]): Unit = {
    validateSource(source)
    factors = source
  }

  override def instrumentPriceSource(source: InstrumentPriceSource[DataFrame]): Unit = {
    validateSource(source)
    prices = source
  }

  override def instrumentModelSource(source: InstrumentModelSource[Model[_]]): Unit = {
    validateSource(source)
    models = source
  }
  
  override def hasSources: Boolean = {
    
  //  println(s"${factors}, ${prices}, ${models}")
    (factors != null && prices != null && models != null)
 }

  override def buildModel(dsCode: String, dsCodes: String*): Unit = { throw new UnsupportedOperationException("Not implemented") }

  //
  private def validateSource(source: Any) = if (source == null) throw new IllegalArgumentException(s"Invalid supplied source ${}")
}