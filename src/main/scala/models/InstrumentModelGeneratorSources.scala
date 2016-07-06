package main.scala.models

import main.scala.factors.RiskFactorSource
import main.scala.prices.InstrumentPriceSource

/**
 * Defines the model generation required data sources. Parameter values specify data source types and model type respectively
 */
trait InstrumentModelGeneratorSources[T, M] {

  /**
   * Source of risk factor data - predictors (features)
   */
  def riskFactorSource(source: RiskFactorSource[T]): Unit

  /**
   * Source of instrument prices - response (label)
   */
  def instrumentPriceSource(source: InstrumentPriceSource[T]): Unit

  /**
   * Access component for persisting generated models
   */
  def instrumentModelSource(source: InstrumentModelSource[M]): Unit

  /**
   * Assertion that all sources have been set
   */
  def hasSources:Boolean
}