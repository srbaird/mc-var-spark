package main.scala.models

trait InstrumentModelGenerator {
  
  /**
   * Create models for supplied dataset codes
   */
  def buildModel(dsCode: String, dsCodes: String*):Unit = buildModel( Array(dsCode) ++ dsCodes)
  
  /**
   * Create models for supplied dataset codes
   */
  def buildModel(dsCodes:Seq[String]):Unit 
}