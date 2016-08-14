package main.scala.models

import org.apache.spark.ml.util.MLReadable
import org.apache.spark.ml.tuning.CrossValidatorModel

class GetCrossValidatorModel extends GetModel[MLReadable[CrossValidatorModel]]  {
  
   override def get = CrossValidatorModel
}