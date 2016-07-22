package main.scala.predict

import scala.slick.driver.MySQLDriver.simple._
import java.sql.Timestamp
import java.sql.Date

class Predictions(tag: Tag) extends Table[( String, Date, Double, Double, Double)](tag, "predictions") {

  def portfolioCode = column[String]("portfolioCode")
  def valueDate = column[Date]("valueDate")
  def hValue = column[Double]("hValue")
  def pValue = column[Double]("pValue")
  def valuation = column[Double]("valuation")

  // Every table needs a * projection with the same type as the table's type parameter
  def * = ( portfolioCode, valueDate, hValue, pValue, valuation)
}
  
