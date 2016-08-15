package main.scala.models

import org.apache.spark.ml.Estimator
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.regression.LinearRegression
import main.scala.application.ApplicationContext
import org.apache.log4j.Logger

/**
 * Cross validator model using linear regression
 */
class CrossValidatorModelEstimator extends ModelEstimator {

   //
  //
  //
  private val logger = Logger.getLogger(getClass)

  override def get: Estimator[_] = {

    logger.trace(s"Create a Cross Validator estimator ")


    val lr = new LinearRegression()

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.standardization, Array(true, false))
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.elasticNetParam, Array(0.1, 0.01))
      .addGrid(lr.maxIter, Array(100))
      .addGrid(lr.tol, Array(1E-6))
      .build()

    new CrossValidator()
      .setEstimator(lr)
      .setEstimatorParamMaps(paramGrid)
      .setEvaluator(new RegressionEvaluator)
      .setNumFolds(5) // Use 'Magic' value = 5
      
  }
}