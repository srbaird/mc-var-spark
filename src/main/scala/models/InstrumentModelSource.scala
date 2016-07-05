package main.scala.models

/**
 * Persistence layer for instrument prediction models
 */
trait InstrumentModelSource[T] {

  /**
   * Supplies all the dataset codes for existing models
   */
  def getAvailableModels: Seq[String]
  
  /**
   * Get a model by dataset code
   */
  def getModel(dsCode:String):Option[T]

}