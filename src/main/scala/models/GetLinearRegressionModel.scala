package main.scala.models

import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.ml.regression.LinearRegressionModel

class GetLinearRegressionModel extends GetModel[MLReadable[LinearRegressionModel]] {

  override def get = LinearRegressionModel
}