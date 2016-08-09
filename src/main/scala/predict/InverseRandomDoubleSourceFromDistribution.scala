package main.scala.predict

import org.apache.commons.math3.distribution.RealDistribution
import org.apache.commons.math3.random.RandomGenerator

/**
 * 
 */
class InverseRandomDoubleSourceFromDistribution(r:RandomGenerator, d:RealDistribution) extends RandomDoubleSource {
  
  override  def nextDouble:Double = d.inverseCumulativeProbability(r.nextDouble) 
}