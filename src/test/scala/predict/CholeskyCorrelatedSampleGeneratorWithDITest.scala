package test.scala.predict

import main.scala.predict.CholeskyCorrelatedSampleGenerator
import test.scala.application.DITestBase
import main.scala.factors.RiskFactorSourceFromFile
import main.scala.predict.RandomDoubleSource
import main.scala.util.Functions.dfToArrayMatrix
import java.time.LocalDate

class CholeskyCorrelatedSampleGeneratorWithDITest extends DITestBase {

  val instanceBeanName = "choleskyCorrelatedSampleGenerator"

  var instance: CholeskyCorrelatedSampleGenerator = _

  override def beforeAll(): Unit = {

    super.beforeAll
  }

  override def beforeEach() {

    generateInstance
  }

  // Overridden to prevent Spark Context from being recycled
  override def afterEach() {}

  //
  //  Start of tests
  //  **************
  /**
   * Invoking sample with a null number of rows less than 1 should result in an exception
   */
  test("test sample with a number of rows less than 1") {

    intercept[IllegalArgumentException] {
      instance.sampleCorrelated(0L, null)
    }
  }

  /**
   * Invoking sample with a null factor matrix should result in an exception
   */
  test("test sample with a null factor matrix") {

    intercept[IllegalArgumentException] {
      instance.sampleCorrelated(1L, null)
    }
  }

  /**
   * Invoking sample with an empty factor matrix should result in an exception
   */
  test("test sample with an empty factor matrix") {

    intercept[IllegalArgumentException] {
      instance.sampleCorrelated(1L, Array[Array[Double]]())
    }
  }

  /**
   * Validate the Cholesky decomposition using a known example
   */
  test("test decomposition with a known single example") {

    instance = new CholeskyCorrelatedSampleGenerator(new KnownExampleRandomDoubleSource)

    val cMartix = Array(Array(0.5411788877189236, 0.06706599060174001, 0.9474708268076227),
      Array(0.32909880856230034, 0.7537504656495697, 0.9749112902308761),
      Array(0.25341313201116333, 0.9983870974281693, 0.5243238207302232))

    val expectedResult = Array(-0.0596691794855918, 0.19283665015241838, -0.38824342176409654)

    val result = instance.sampleCorrelated(cMartix)

    assert(result.length == 1)

    result(0).foreach { v => assert(v == expectedResult(result(0).indexOf(v))) }

  }

  /**
   * Test that the correct number of sample rows is returned
   */
  test("test decomposition with more than one row") {

    val expectedNumRows = 4L
    instance = new CholeskyCorrelatedSampleGenerator(new KnownExampleRandomDoubleSource)

    val cMartix = Array(Array(0.5411788877189236, 0.06706599060174001, 0.9474708268076227),
      Array(0.32909880856230034, 0.7537504656495697, 0.9749112902308761),
      Array(0.25341313201116333, 0.9983870974281693, 0.5243238207302232))

    val expectedResult = Array(-0.0596691794855918, 0.19283665015241838, -0.38824342176409654)

    val result = instance.sampleCorrelated(expectedNumRows, cMartix)

    assert(result.length == expectedNumRows)

    result.foreach { r => r.foreach { v => assert(v == expectedResult(result(0).indexOf(v))) } }
  }

  /**
   * Test sampling from the risk factors source
   */
  test("test sampling from the risk factors source from 01Jun15 to 31May16") {

    val colNameToDrop = "valueDate" // Used as a key in joins to instrument prices
    val factors = new RiskFactorSourceFromFile()
    val f = factors.factors(LocalDate.of(2015, 6, 1), LocalDate.of(2016, 5, 31)).drop(colNameToDrop)

    val fAsMatrix = dfToArrayMatrix(f)

    val result = instance.sampleCorrelated(dfToArrayMatrix(f))

    val expectedNumberOfSamples = f.columns.length

    assert(result.length == 1)
    assert(result(0).length == expectedNumberOfSamples)
  }

  /**
   * Test sampling 10000 rows from the risk factors source
   */
  test("test sampling 10000 rows from the risk factors source from 01Jun15 to 31May16") {

    val colNameToDrop = "valueDate" // Used as a key in joins to instrument prices
    val factors = new RiskFactorSourceFromFile()
    val f = factors.factors(LocalDate.of(2015, 6, 1), LocalDate.of(2016, 5, 31)).drop(colNameToDrop)

    val numRowsToCreate = 10000L
    val expectedNumberOfSamples = f.columns.length

    val startTime = System.currentTimeMillis()
    val result = instance.sampleCorrelated(numRowsToCreate, dfToArrayMatrix(f))
    val endTime = System.currentTimeMillis()

    System.out.println(s"${numRowsToCreate} rows took ${endTime - startTime}(ms)")

    assert(result.length == numRowsToCreate)
    assert(result(0).length == expectedNumberOfSamples)
  }

  //
  // 
  //
  class KnownExampleRandomDoubleSource extends RandomDoubleSource {

    val fixedRandoms = Array(-0.4, -1.7, 1.9)

    private var index = -1
    override def nextDouble: Double = { index += 1; fixedRandoms(index % 3) }
    override def randomMatrix(rows: Long, cols: Long): Array[Array[Double]] = {

      (1L to rows).map(l => (1L to cols).map(i => nextDouble).toArray).toArray

    }

  }
  //
  //  Helper functions
  //  ****************
  private def generateInstance = {

    super.generateApplicationContext
    instance = ctx.getBean(instanceBeanName).asInstanceOf[CholeskyCorrelatedSampleGenerator]
  }
}