package main.scala.models

import org.apache.spark.ml.Estimator
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.log4j.Logger
import main.scala.application.ApplicationContext
import org.apache.spark.ml.Pipeline

class LinearRegressionModelEstimator extends ModelEstimator {

  //
  //
  //
  lazy val labelColumn = ApplicationContext.getContext.getString("instrumentPrice.valueColumn")
  lazy val keyColumn = ApplicationContext.getContext.getString("instrumentPrice.keyColumn")
  val modelLabelColumn = "label"

  private val logger = Logger.getLogger(getClass)

  override def get: Estimator[_] = {
    logger.trace(s"Create a Linear Regression estimator ")

    new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol(modelLabelColumn)
      .setRegParam(0.15)
      .setElasticNetParam(0.0)
      .setMaxIter(100)
      .setTol(1E-6)
  }
}