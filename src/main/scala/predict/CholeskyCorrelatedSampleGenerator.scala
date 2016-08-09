package main.scala.predict

import org.apache.commons.math3.linear.Array2DRowRealMatrix
import org.apache.commons.math3.linear.CholeskyDecomposition
import org.apache.commons.math3.stat.correlation.Covariance
import org.apache.log4j.Logger
import main.scala.application.ApplicationContext

class CholeskyCorrelatedSampleGenerator(r: RandomDoubleSource) extends CorrelatedSampleGenerator {

  //
  //
  //
  private val logger = Logger.getLogger(getClass)
  /**
   *
   */
  override def sampleCorrelated(n: Long, f: Array[Array[Double]]): Array[Array[Double]] = {

    logger.debug(s"Generate ${n} correlated samples")
    // The number of rows must be at least 1
    if (!(n >= 1)) {
      throw new IllegalArgumentException(s"Invalid number of rows supplied: ${n}")
    }

    // The factor array must not be null or empty
    if (f == null || f.isEmpty) {
      throw new IllegalArgumentException(s"Invalid factor matrix supplied: ${n}")
    }
    //
    // Generate a Covariance
    //
    logger.trace(s"Create a covariance instance")
    val c = new Covariance(f)
    val fCovariance = c.getCovarianceMatrix
    val numOfFactors = fCovariance.getData.length
    //
    // Get the Cholesky decomposition 
    //
    logger.trace(s"Create a Cholesky Decomposition instance")
    val cholesky = new CholeskyDecomposition(fCovariance)

    val decomposition = cholesky.getLT
    //
    // Generate n x numOfFactors matrix of random samples
    //
    logger.trace(s"Generate the observations")
    val observationRangeAsRDD = ApplicationContext.sc.parallelize(1L to n)
    //    val observations = (1L to n).map(l => (1 to numOfFactors).map(i => r.nextDouble).toArray).toArray
    //   val observations = observationRangeAsRDD.map { x => (1 to numOfFactors).map(i => r.nextDouble).toArray }.collect()
    val observations = r.randomMatrix(n, numOfFactors.toLong)
    //
    // Convert to Matrix for multiplication
    //
    val observationsAsMAtrix = new Array2DRowRealMatrix(observations)
    //
    // Multiply and return the resulting Array
    //
    observationsAsMAtrix.multiply(decomposition).getData
  }
}