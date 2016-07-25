package main.scala.predict

import scala.slick.driver.MySQLDriver.simple._
import scala.slick.lifted.TableQuery
import main.scala.application.ApplicationContext
import java.time.LocalDate
import java.time.ZoneId
import java.sql.Date

import scala.slick.driver.MySQLDriver.simple._

class MySQLPredictionPersistor extends PredictionPersistor {

  val appContext = ApplicationContext.getContext

  val sc = ApplicationContext.sc
  //
  // Context variables to connect to the database
  //
  lazy val dbUrl = appContext.getString("predictions.logDBUrl")
  lazy val dbDriver = appContext.getString("predictions.logDBDriver")
  lazy val dbUser = appContext.getString("predictions.dbUser")
  lazy val dbPassword = appContext.getString("predictions.dbPassword")

  // The query interface for the Predictions table
  val predictions: TableQuery[Predictions] = TableQuery[Predictions]

  /**
   * 
   */
  def persist(portfolioCode: String, at: LocalDate, eClass:String, hValue: Double, pValue: Double, valuation: Double) = {

    Database.forURL(dbUrl, driver = dbDriver, user = dbUser, password = dbPassword) withSession { implicit session =>

      val d = at.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli()

      predictions += (portfolioCode, new Date(d), eClass, hValue, pValue, valuation)
    }
  }

}