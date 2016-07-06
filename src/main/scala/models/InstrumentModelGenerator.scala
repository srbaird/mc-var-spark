package main.scala.models

trait InstrumentModelGenerator {
  
  /**
   * Create models for supplied dataset codes
   */
  def buildModel(dsCode: String, dsCodes: String*):Map[String,(Boolean,String)] = buildModel( Array(dsCode) ++ dsCodes)
  
  /**
   * Create models for supplied dataset codes
   */
  def buildModel(dsCodes:Seq[String]):Map[String,(Boolean,String)] 
}