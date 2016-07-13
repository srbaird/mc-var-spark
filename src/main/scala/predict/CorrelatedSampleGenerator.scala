package main.scala.predict

/**
 * Generate random samples of correlated factor variates
 */
trait CorrelatedSampleGenerator {

  val defaultNumberOfVariates = 1L
  /**
   * Generate a default number of variates correlated to the supplied matrix
   */
  def sampleCorrelated(f: Array[Array[Double]]): Array[Array[Double]] = sampleCorrelated(defaultNumberOfVariates, f)

  /**
   * Generate n number of variates correlated to the supplied matrix
   */
  def sampleCorrelated(n: Long, f: Array[Array[Double]]): Array[Array[Double]]

}