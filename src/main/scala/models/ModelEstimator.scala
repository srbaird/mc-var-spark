package main.scala.models

import org.apache.spark.ml.Estimator
import org.apache.spark.sql.DataFrame

trait ModelEstimator {

  def get: Estimator[_]
}