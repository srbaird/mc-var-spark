package test.scala.predict

import main.scala.predict.CholeskyCorrelatedSampleGenerator
import org.scalatest.FunSuite
import org.scalatest.Suite
import main.scala.predict.RandomDoubleSourceFromRandom
import org.apache.commons.math3.random.ISAACRandom
import main.scala.predict.RandomDoubleSource

class CholeskyCorrelatedSampleGeneratorTest extends FunSuite { self: Suite =>

  var instance: CholeskyCorrelatedSampleGenerator = _

//  override def beforeEach() {

//    instance = new CholeskyCorrelatedSampleGenerator(new RandomDoubleSourceFromRandom(new ISAACRandom()))
//  }

  /**
   * Validate the Cholesky decomposition using a known example
   */
  test("test decomposition with known examples") {

    instance = new CholeskyCorrelatedSampleGenerator(new KnownExampleRandomDoubleSource)

    val cMartix = Array(Array(0.5411788877189236, 0.06706599060174001, 0.9474708268076227),
      Array(0.32909880856230034, 0.7537504656495697, 0.9749112902308761),
      Array(0.25341313201116333, 0.9983870974281693, 0.5243238207302232))

    val expectedResult = Array(-0.0596691794855918,0.19283665015241838,-0.38824342176409654)

    val result = instance.sampleCorrelated(cMartix)
    
   // assert(result(0) == expectedResult)

    result(0).foreach { v =>  assert(v == expectedResult(result(0).indexOf(v))) }


  }

  class KnownExampleRandomDoubleSource extends RandomDoubleSource {

    val fixedRandoms = Array(-0.4, -1.7, 1.9)

    private var index = -1
    override def nextDouble: Double = { index += 1; fixedRandoms(index % 3) }

  }

}