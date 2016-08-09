package main.scala.predict

/**
 * Simplification of all random number generation classes into a single method
 */
trait RandomDoubleSource {
  
  def nextDouble:Double
  
  def randomMatrix(rows:Long, cols:Long):Array[Array[Double]]
  
}