package test.scala.factors

import org.apache.commons.math3.distribution.MultivariateNormalDistribution
import org.apache.commons.math3.linear.Array2DRowRealMatrix
import org.apache.commons.math3.linear.CholeskyDecomposition
import org.apache.commons.math3.random.ISAACRandom
import org.apache.commons.math3.random.RandomAdaptor
import org.apache.commons.math3.stat.correlation.Covariance
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation
import org.apache.spark.annotation.Experimental
import main.scala.application.ApplicationContext
import main.scala.factors.RiskFactorSourceFromFile
import main.scala.transform.ValueDateTransformer
import test.scala.application.SparkTestBase
import org.apache.commons.math3.stat.correlation.SpearmansCorrelation
import org.apache.commons.math3.distribution.TDistribution
import org.apache.spark.sql.DataFrame
import java.time.LocalDate
import scala.util.Random
import scala.util.control.Breaks._

class MultivariateNormalDistributionTest extends SparkTestBase {

  var instance: RiskFactorSourceFromFile = _
  //
  var hdfsLocation: String = _
  var fileLocation: String = _
  var factorsFileName: String = _

  override def beforeAll(): Unit = {

    super.beforeAll()
  }

  override def beforeEach() {

    generateContextFileContentValues

    generateContextFileContents

    generateAppContext

    generateDefaultInstance
  }

  // Prevent the Spark Context being recycled
  override def afterEach() {}

  // From https://gist.github.com/frgomes/c6bf34eeb5ae1769b072 -  Added String
  // Imlicit and Explicit conversions to Double
  def toDouble: (Any) => Double = { case i: Int => i case f: Float => f case d: Double => d case s: String => s.toDouble }

  def sFunc(w: Seq[Double]): Double = (w.last - w.head) / w.head

  // Apply a sliding function by column over a 2D matrix of doubles  
  def slider(s: Int, m: Array[Array[Double]], f: Seq[Double] => Double): Array[Array[Double]] = {
    m.transpose.map { r => r.sliding(s).map { w => f(w) }.toArray }.transpose
  }

  // Collect the DataFrame into an 2D Array of Double
  def dfToArrayMatrix(df: DataFrame): Array[Array[Double]] = {

    df.collect.toArray.map { row => row.toSeq.toArray.map { x => toDouble(x).asInstanceOf[Double] } }
  }

  /**
   *
   */
  test("test multivariate distribution") {

    // From Advanced Analytics with Spark

    val factors = instance.factors().drop("valueDate")
    val selectCols = factors.columns
    val riskFactors = factors.select(selectCols.head, selectCols.tail: _*)

    val factorMatrix = riskFactors.collect.toArray.map { row => row.toSeq.toArray.map { x => toDouble(x).asInstanceOf[Double] } }

    val factorCov = new Covariance(factorMatrix).getCovarianceMatrix.getData

    //    println(s"Covariance has ${factorCov.length} rows of ${factorCov(0).length} columns")

    //   val factorMeans = riskFactors.columns.map { colName => riskFactors(colName)}
    val factorMeans = riskFactors.describe().where("summary = \"mean\"").drop("summary").head().toSeq.toArray.map { x => toDouble(x).asInstanceOf[Double] }

    //    println(s"${factorCov}")
    val factorDist = new MultivariateNormalDistribution(factorMeans, factorCov)

    //println(s"${factorDist.sample}")
    //    println(s"Number of sample points: ${factorDist.sample.length}")
    //    println(factorDist.sample.mkString(", "))
  }

  /**
   *
   */
  test("test multivariate Cholesky decomposition") {

    val factors = instance.factors().drop("valueDate")
    val selectCols = factors.columns
    val riskFactors = factors.select(selectCols.head, selectCols.tail: _*)

    val numOfFactors = selectCols.length

    //     
    val factorsAsMatrix = riskFactors.collect.toArray.map { row => row.toSeq.toArray.map { x => toDouble(x).asInstanceOf[Double] } }
    val factorsAsRealMatrix = new Array2DRowRealMatrix(factorsAsMatrix)

    val factorCov = new Covariance(factorsAsMatrix).getCovarianceMatrix
    //    val factorCov = new SpearmansCorrelation(factorsAsRealMatrix).getCorrelationMatrix
    // val factorCov = new PearsonsCorrelation(factorsAsRealMatrix).getCorrelationMatrix
    //   println("Correlation")

    //    println(factorCov.getRow(0).mkString(", "))

    val cholesky = new CholeskyDecomposition(factorCov)

    val decomposition = cholesky.getL

    //   decomposition.getData.foreach { row => println(row.mkString(", ")) }

    //   val recomposition = cholesky.getL.multiply(cholesky.getLT)
    //   println(s"Recomposition - Rows: ${recomposition.getRowDimension}, Cols: ${recomposition.getColumnDimension}")
    //   println(recomposition.getRow(0).mkString(", "))

    //    println(s"Decomposition - Rows: ${decomposition.getRowDimension}, Cols: ${decomposition.getColumnDimension}")
    //   println(decomposition.getRow(0).mkString(", "))

    // Draw a vector of random obervations
    val random = new RandomAdaptor(new ISAACRandom())

    // Initially use the normal distribution
    //   val obs = (0 until numOfFactors).map(i => random.nextGaussian())
    val realDistribution = new TDistribution(new ISAACRandom(), 4)

    val obs = (0 until numOfFactors).map(i => realDistribution.sample())

    val nObs = (1 to 1000).map(n => (0 until numOfFactors).map(i => realDistribution.sample()).toArray).toArray

    //    println(s"${obs.size} random observations generated")
    //    println(obs.mkString(", "))

    //   val obsAsRealMatrix = new Array2DRowRealMatrix(Array[Array[Double]](obs.toArray))
    val obsAsRealMatrix = new Array2DRowRealMatrix(nObs)

    // Apply the decomposition to the observations
    val correlatedObservations = obsAsRealMatrix.multiply(decomposition)

    //    println(s"Rows: ${correlatedObservations.getRowDimension}, Cols: ${correlatedObservations.getColumnDimension}")
    //    println(correlatedObservations.getRow(0).mkString(", "))

  }

  /**
   *
   */
  test("test sliding") {

    val riskFactors = instance.factors(LocalDate.now().minusYears(1)).drop("valueDate")

    val numOfFactors = riskFactors.columns.length

    val factorsAsMatrix = dfToArrayMatrix(riskFactors)

    val hDay = slider(10, factorsAsMatrix, w => w.last - w.head)

    val factorCov = new Covariance(hDay).getCovarianceMatrix

    val cholesky = new CholeskyDecomposition(factorCov)

    val decomposition = cholesky.getLT

    val random = new RandomAdaptor(new ISAACRandom())

    // Initially use the normal distribution
    val nObs = (1 to 1).map(n => (1 to numOfFactors).map(i => random.nextGaussian()).toArray).toArray

    // Apply the decomposition to the observations

    val obsAsRealMatrix = new Array2DRowRealMatrix(nObs)
    val correlatedObservations = new Array2DRowRealMatrix(obsAsRealMatrix.getData).multiply(decomposition)

    //    println(s"Rows: ${correlatedObservations.getRowDimension}, Cols: ${correlatedObservations.getColumnDimension}")
    correlatedObservations.getData.foreach(r => println(r.mkString(", ")))
  }
  /**
   *
   */
  test("test cholesky") {

    // Using example from Wikipedia
    val cMartix = Array(Array(4D, 12D, -16D), Array(12D, 37D, -43D), Array(-16D, -43D, 98D))

    val cholesky = new CholeskyDecomposition(new Array2DRowRealMatrix(cMartix))
    val c = cholesky.getL
    //    println(c.getRow(0).mkString(", "))
    //    println(c.getRow(1).mkString(", "))
    //    println(c.getRow(2).mkString(", "))

    // from http://www.sitmo.com/?p=720
    val cMartix2 = Array(Array(1, 0.6, 0.3), Array(0.6, 1, 0.5), Array(0.3, 0.5, 1))
    val cholesky2 = new CholeskyDecomposition(new Array2DRowRealMatrix(cMartix2))
    val c2 = cholesky2.getL
    println(c2.getRow(0).mkString(", "))
    println(c2.getRow(1).mkString(", "))
    println(c2.getRow(2).mkString(", "))

    val rand = new Array2DRowRealMatrix(Array(-0.3999, -1.6041, -1.0106))

    val correlatedObservations = c2.multiply(rand)
    correlatedObservations.getData.foreach { x => println(x.mkString(", ")) }

  }

  /**
   *
   */
  test("look for positive definite matrix") {

    // Using example from Wikipedia

    val rx = new Random

    breakable {
      for (i <- 1 to 100000) {

        val tMartix = Array(Array(rx.nextDouble(), rx.nextDouble(), rx.nextDouble()),
          Array(rx.nextDouble(), rx.nextDouble(), rx.nextDouble()),
          Array(rx.nextDouble(), rx.nextDouble(), rx.nextDouble()))

        val factorCov = new Covariance(tMartix).getCovarianceMatrix
        try {
          val cholesky = new CholeskyDecomposition(factorCov)
          println("Found one...")
          tMartix.foreach { x => println(x.mkString(", ")) }
          break
        } catch {
          case allExceptions: Throwable =>

        }

      }
    }

    //    val c = cholesky.getL

    //    val rand = new Array2DRowRealMatrix(Array(-0.4, -1.7, 1.9))

    //    val correlatedObservations = c.multiply(rand)
    //    correlatedObservations.getData.foreach { x => println(x.mkString(", ")) }

  }

  test("test cholesky with covariance") {

    val tMartix = Array(Array(0.5411788877189236, 0.06706599060174001, 0.9474708268076227),
      Array(0.32909880856230034, 0.7537504656495697, 0.9749112902308761),
      Array(0.25341313201116333, 0.9983870974281693, 0.5243238207302232))

    val factorCov = new Covariance(tMartix).getCovarianceMatrix
    val cholesky = new CholeskyDecomposition(factorCov)

    val c = cholesky.getL

    val rand = new Array2DRowRealMatrix(Array(-0.4, -1.7, 1.9))

    val correlatedObservations = c.multiply(rand)
    correlatedObservations.getData.foreach { x => println(x.mkString(", ")) }

  }

  /**
   *
   */
  test("test h-day covariance matrix with multivariate distribution ") {

    val riskFactors = instance.factors(LocalDate.now().minusYears(1)).drop("valueDate")

    val numOfFactors = riskFactors.columns.length

    val factorsAsMatrix = dfToArrayMatrix(riskFactors)

    val hDay = slider(10, factorsAsMatrix, w => w.last - w.head)

    val factorCov = new Covariance(hDay).getCovarianceMatrix

    val factorMeans = hDay.transpose.map { m => m.foldLeft(0D) { (acc, e) => acc + e } / m.length }

    val factorDist = new MultivariateNormalDistribution(new ISAACRandom(), factorMeans, factorCov.getData)

    println(factorMeans.mkString(", "))
    println(factorDist.sample.mkString(", "))

  }

  /**
   *
   */
  test("test transposition") {

    val cMatrix = Array(Array(1.0, 5.0, -5.0), Array(2.0, 6.0, -10.0), Array(3.0, 7.0, -15.0), Array(4.0, 8.0, -20.0))
    slider(2, cMatrix, w => (w.last - w.head) / w.last).foreach(r => println(r.mkString(", ")))

  }

  /**
   *
   */
  test("test array mean") {

    val cMatrix = Array(Array(1.0, 5.0, -5.0), Array(2.0, 6.0, -10.0), Array(3.0, 7.0, -15.0), Array(4.0, 8.0, -20.0))
    val cMeans = cMatrix.transpose.map { m => m.foldLeft(0D) { (acc, e) => acc + e } / m.length }
    println(cMeans.mkString(", "))

  }

  //
  // Helper methods to create  valid test environment
  //
  private def generateContextFileContentValues = {

    hdfsLocation = "\"hdfs://localhost:54310\""
    fileLocation = "\"/project/test/initial-testing/\""
    factorsFileName = "\"factors.clean.csv\""
  }

  private def generateContextFileContents: String = {

    val configFileContents = s"riskFactor{hdfsLocation = ${hdfsLocation}, fileLocation = ${fileLocation}, factorsFileName = ${factorsFileName} }"

    s"${hadoopAppContextEntry}, ${configFileContents}" // Prepend the Hadoop dependencies
  }

  private def generateDefaultInstance = {

    instance = new RiskFactorSourceFromFile(sc)
    // TODO: move the dependencies to DI implementation
    instance.add(new ValueDateTransformer())
  }

  private def generateAppContext {

    val configFile = writeTempFile(generateContextFileContents)
    try {
      val result = ApplicationContext.useConfigFile(configFile)
    } finally {
      configFile.delete()
    }
  }
}