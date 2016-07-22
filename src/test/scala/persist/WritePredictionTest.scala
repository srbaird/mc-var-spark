package test.scala.persist

import scala.slick.driver.MySQLDriver.simple._
import scala.slick.lifted.TableQuery
import main.scala.predict.Predictions
import test.scala.application.SparkTestBase
import java.sql.Timestamp
import java.sql.Date
import java.time.LocalDate
import java.time.ZoneOffset
import java.time.ZoneId

class WritePredictionTest extends SparkTestBase {

  // The query interface for the Suppliers table
  val predictions: TableQuery[Predictions] = TableQuery[Predictions]

  override def beforeAll(): Unit = {}

  override def beforeEach() {}

  // Prevent the Spark Context being recycled
  override def afterEach() {}

  /**
   * Write a row to the table
   */
  test("write a prediction row ") {

    Database.forURL("jdbc:mysql://localhost:3306/pData", driver = "com.mysql.jdbc.Driver", user = "root", password = "nbuser") withSession {
      implicit session =>

//        (predictions.ddl).create
        val d = LocalDate.now().atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli()
        
        predictions += ("Portfolio code", new Date(d), 10D, 95D, 86504.11)
 
//        (predictions.ddl).drop
    }
  }

}