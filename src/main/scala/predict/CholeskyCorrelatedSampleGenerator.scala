package main.scala.predict

import org.apache.commons.math3.random.ISAACRandom
import org.apache.commons.math3.random.RandomAdaptor
import org.apache.commons.math3.linear.CholeskyDecomposition
import org.apache.commons.math3.stat.correlation.Covariance
import org.apache.commons.math3.linear.Array2DRowRealMatrix

class CholeskyCorrelatedSampleGenerator(r: RandomDoubleSource) extends CorrelatedSampleGenerator {

  override def sampleCorrelated(n: Long, f: Array[Array[Double]]): Array[Array[Double]] = {

    val c = new Covariance(f)
    val fCovariance = c.getCovarianceMatrix
    val numOfFactors = c.getN
    //
    // Get the Cholesky decomposition 
    //
    val cholesky = new CholeskyDecomposition(fCovariance)

    val decomposition = cholesky.getLT
    //
    // Generate n x numOfFactors matrix of random samples
    //
    val observations = (1L to n).map(l => (1 to numOfFactors).map(i => r.nextDouble).toArray).toArray
    //
    // Convert to MAtrix for multiplication
    //
    val observationsAsMAtrix = new Array2DRowRealMatrix(observations)
    //
    // Multiply and return the resulting Array
    //
    observationsAsMAtrix.multiply(decomposition).getData

  }

}