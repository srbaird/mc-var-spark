package main.scala.models

trait InstrumentModelGenerator {
  
  /**
   * Create models for supplied dataset codes
   */
  def buildModel(dsCode: String, dsCodes: String*):Unit
  
  /**
   * Create models for supplied dataset codes
   */
  def buildModel(dsCodes:Seq[String]):Unit = buildModel(dsCodes.head, dsCodes.tail: _*)
}