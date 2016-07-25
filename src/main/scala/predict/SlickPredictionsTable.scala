package main.scala.predict

import scala.slick.driver.MySQLDriver.simple._
import java.sql.Timestamp
import java.sql.Date

class Predictions(tag: Tag) extends Table[( String, Date, String, Double, Double, Double)](tag, "predictions") {

  def portfolioCode = column[String]("portfolioCode")
  def valueDate = column[Date]("valueDate")
  def eClass = column[String]("eClass")
  def hValue = column[Double]("hValue")
  def pValue = column[Double]("pValue")
  def valuation = column[Double]("valuation")

  def * = ( portfolioCode, valueDate, eClass, hValue, pValue, valuation)
}
  
