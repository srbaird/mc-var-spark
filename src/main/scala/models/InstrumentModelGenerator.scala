package main.scala.models

import java.time.LocalDate

trait InstrumentModelGenerator {

  /**
   * Create models for supplied dataset codes
   */
  def buildModel(dsCode: String, dsCodes: String*): Map[String, (Boolean, String)] = buildModel(Array(dsCode) ++ dsCodes)

  /**
   * Create models for supplied dataset codes with a from-date
   */
  def buildModel(from: LocalDate, dsCode: String, dsCodes: String*): Map[String, (Boolean, String)] = buildModel(from, null, Array(dsCode) ++ dsCodes)

  /**
   * Create models for supplied dataset codes with a from-date and a to-date
   */
  def buildModel(from: LocalDate, to: LocalDate, dsCode: String, dsCodes: String*): Map[String, (Boolean, String)] = buildModel(from, to, Array(dsCode) ++ dsCodes)

  /**
   * Create models for supplied dataset codes
   */
  def buildModel(dsCodes: Seq[String]): Map[String, (Boolean, String)] = buildModel(null, null, dsCodes)

  /**
   * Create models for supplied dataset codes with a from-date
   */
  def buildModel(from: LocalDate, dsCodes: Seq[String]): Map[String, (Boolean, String)] = buildModel(from, null, dsCodes)

  /**
   * Create models for supplied dataset codes with a from-date and a to-date
   */
  def buildModel(from: LocalDate, to: LocalDate, dsCodes: Seq[String]): Map[String, (Boolean, String)]
}