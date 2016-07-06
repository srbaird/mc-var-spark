package main.scala.models

import main.scala.factors.RiskFactorSource
import main.scala.prices.InstrumentPriceSource

/**
 * Defines the model generation required data sources
 */
trait InstrumentModelGeneratorSources {

  /**
   * Source of risk factor data - predictors (features)
   */
  def riskFactorSource(source: RiskFactorSource[_]): Unit

  /**
   * Source of instrument prices - response (label)
   */
  def instrumentPriceSource(source: InstrumentPriceSource[_]): Unit

  /**
   * Access component for persisting generated models
   */
  def instrumentModelSource(source: InstrumentModelSource[_]): Unit

  /**
   * Assertion that all sources have been set
   */
  def hasSources:Boolean
}