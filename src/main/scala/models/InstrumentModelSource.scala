package main.scala.models

/**
 * Persistence layer for instrument prediction models
 */
trait InstrumentModelSource {

  /**
   * Supplies all the dataset codes for existing models
   */
  def getAvailableModels: Seq[String]

}