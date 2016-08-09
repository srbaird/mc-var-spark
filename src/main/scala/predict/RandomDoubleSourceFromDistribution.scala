package main.scala.predict

import org.apache.commons.math3.distribution.RealDistribution
import org.apache.commons.math3.random.RandomGenerator
import main.scala.application.ApplicationContext

class RandomDoubleSourceFromDistribution(r: RandomGenerator, d: RealDistribution) extends RandomDoubleSource {

  override def nextDouble: Double = d.density(r.nextDouble)

  override def randomMatrix(rows: Long, cols: Long): Array[Array[Double]] = {

    val rowsRangeAsRDD = ApplicationContext.sc.parallelize(1L to rows)
    rowsRangeAsRDD.map { x => (1L to cols).map(i => nextDouble).toArray }.collect()
  }

}