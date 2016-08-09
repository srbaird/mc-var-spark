package main.scala.predict

import org.apache.commons.math3.random.RandomAdaptor
import main.scala.application.ApplicationContext

class RandomDoubleSourceFromAdapter(a:RandomAdaptor) extends RandomDoubleSource {
  
  override  def nextDouble:Double = a.nextDouble() 
  
    override def randomMatrix(rows: Long, cols: Long): Array[Array[Double]] = {

    val rowsRangeAsRDD = ApplicationContext.sc.parallelize(1L to rows)
    rowsRangeAsRDD.map { x => (1L to cols).map(i => nextDouble).toArray }.collect()
  }
  
}