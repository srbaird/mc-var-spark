package main.scala.predict

import org.apache.commons.math3.random.RandomAdaptor

class RandomDoubleSourceFromAdapter(a:RandomAdaptor) extends RandomDoubleSource {
  
  override  def nextDouble:Double = a.nextDouble() 
  
}