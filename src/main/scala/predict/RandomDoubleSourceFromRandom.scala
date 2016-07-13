package main.scala.predict

import java.util.Random
import org.apache.commons.math3.random.RandomGenerator

class RandomDoubleSourceFromRandom(r:RandomGenerator) extends RandomDoubleSource{
  
  override  def nextDouble:Double = r.nextDouble
  
}