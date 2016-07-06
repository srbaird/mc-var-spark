package main.scala.models

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.Model
import org.apache.spark.sql.DataFrame

import main.scala.factors.RiskFactorSource
import main.scala.prices.InstrumentPriceSource

/**
 * For want of a better name, the default model generator for data sets
 */
class DefaultInstrumentModelGenerator(sc: SparkContext) extends InstrumentModelGenerator with InstrumentModelGeneratorSources {

  // Source of Market Risk Factor data (features)
  private var factors: RiskFactorSource[DataFrame] = _

  // Source of Instrument prices data (label)
  private var prices: InstrumentPriceSource[DataFrame] = _

  // Destination of generated models
  private var models: InstrumentModelSource[Model[_]] = _

  override def riskFactorSource(source: RiskFactorSource[_]): Unit = { throw new UnsupportedOperationException("Not implemented") }

  override def instrumentPriceSource(source: InstrumentPriceSource[_]): Unit = { throw new UnsupportedOperationException("Not implemented") }

  override def instrumentModelSource(source: InstrumentModelSource[_]): Unit = { throw new UnsupportedOperationException("Not implemented") }
  
  override def hasSources:Boolean = { throw new UnsupportedOperationException("Not implemented") }
  
  override def buildModel(dsCode: String, dsCodes: String*):Unit = { throw new UnsupportedOperationException("Not implemented") }

}