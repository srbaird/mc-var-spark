package main.scala.predict

import org.apache.commons.math3.distribution.RealDistribution
import org.apache.commons.math3.random.RandomGenerator

class RandomDoubleSourceFromDistribution(r:RandomGenerator, d:RealDistribution) extends RandomDoubleSource  {
  
  override  def nextDouble:Double = d.density(r.nextDouble) 
  
}