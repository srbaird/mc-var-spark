package main.scala.models

import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.ml.tuning.TrainValidationSplitModel

class GetTrainValidationSplitModel extends GetModel[MLReadable[TrainValidationSplitModel]]  {
  
   override def get = TrainValidationSplitModel
}