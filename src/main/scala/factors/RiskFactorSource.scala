package main.scala.factors

import java.time.LocalDate

/**
 * Provides a matrix of market risk factors for model training and prediction
 */
trait RiskFactorSource[T] {

  /**
   * Return an 1 x n matrix of the most recent risk factors
   */
  def head(): T = head(1)

  /**
   * Return an rows x n matrix of the most recent risk factors
   */
  def head(rows: Int): T

  /**
   * Return an n x n matrix of risk factors starting at the supplied date
   */
  def factors(from: LocalDate): T = factors(from, null)
  
  /**
   * Return an n x n matrix of risk factors between the supplied dates
   */
  def factors(from: LocalDate, to: LocalDate): T

}