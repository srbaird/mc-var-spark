package main.scala.predict

import org.apache.commons.math3.linear.Array2DRowRealMatrix
import org.apache.commons.math3.linear.CholeskyDecomposition
import org.apache.commons.math3.stat.correlation.Covariance

class CholeskyCorrelatedSampleGenerator(r: RandomDoubleSource) extends CorrelatedSampleGenerator {

  /**
   *
   */
  override def sampleCorrelated(n: Long, f: Array[Array[Double]]): Array[Array[Double]] = {

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
    val c = new Covariance(f)
    val fCovariance = c.getCovarianceMatrix
    val numOfFactors = fCovariance.getData.length
    //
    // Get the Cholesky decomposition 
    //
    val cholesky = new CholeskyDecomposition(fCovariance)

    val decomposition = cholesky.getLT
    //
    // Generate n x numOfFactors matrix of random samples
    //
    val observations = (1L to n).map(l => (1 to numOfFactors).map(i => (r.nextDouble - 0.5) * 2 ).toArray).toArray
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