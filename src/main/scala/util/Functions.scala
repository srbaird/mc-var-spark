package main.scala.util

import org.apache.spark.sql.DataFrame

object Functions {

  /**
   *  Implicit and Explicit conversions to Double
   *  Taken from https://gist.github.com/frgomes/c6bf34eeb5ae1769b072 -  Added String
   */
  def toDouble: (Any) => Double = { case i: Int => i case f: Float => f case d: Double => d case s: String => s.toDouble }

  /**
   * Collect a DataFrame into an 2D Array of Doubles
   */
  def dfToArrayMatrix(df: DataFrame): Array[Array[Double]] = {

    df.collect.toArray.map { row => row.toSeq.toArray.map { x => toDouble(x).asInstanceOf[Double] } }
  }

  /**
   * Apply a function by column over a window onto a 2D matrix of doubles
   */
  def window(s: Int, m: Array[Array[Double]], f: Seq[Double] => Double): Array[Array[Double]] = {
    m.transpose.map { r => r.sliding(s).map { w => f(w) }.toArray }.transpose
  }
}