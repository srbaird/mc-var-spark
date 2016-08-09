package main.scala.predict

import java.util.Random
import org.apache.commons.math3.random.RandomGenerator
import main.scala.application.ApplicationContext

class RandomDoubleSourceFromRandom(r:RandomGenerator) extends RandomDoubleSource{
  
  override  def nextDouble:Double = r.nextDouble 
  
    override def randomMatrix(rows: Long, cols: Long): Array[Array[Double]] = {

    val rowsRangeAsRDD = ApplicationContext.sc.parallelize(1L to rows)
    rowsRangeAsRDD.map { x => (1L to cols).map(i => nextDouble).toArray }.collect()
  }
  
}