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
  def getModel(dsCode: String): Option[T]

  /**
   * Persist a model. If the model already exists it will be replaced
   */
  def putModel(dsCode: String, model: T): Unit

  /**
   * Remove model.
   */
  def removeModel(dsCode: String): Unit
}