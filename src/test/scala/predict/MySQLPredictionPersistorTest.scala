package test.scala.predict

import scala.slick.driver.MySQLDriver.simple._
import scala.slick.lifted.TableQuery
import main.scala.predict.Predictions
import test.scala.application.SparkTestBase
import java.sql.Timestamp
import java.sql.Date
import java.time.LocalDate
import java.time.ZoneOffset
import java.time.ZoneId
import main.scala.predict.MySQLPredictionPersistor
import main.scala.predict.HDayMCSValuePredictor
import main.scala.models.InstrumentModelSourceFromFile
import main.scala.predict.RandomDoubleSourceFromRandom
import main.scala.portfolios.PortfolioValuesSourceFromFile
import main.scala.factors.RiskFactorSourceFromFile
import main.scala.predict.CholeskyCorrelatedSampleGenerator
import org.apache.commons.math3.random.ISAACRandom
import main.scala.application.ApplicationContext

class MySQLPredictionPersistorTest extends SparkTestBase {

  var instance: MySQLPredictionPersistor = _

  var dbUrl: String = _
  var dbDriver: String = _
  var dbUser: String = _
  var dbPassword: String = _

  // Override until a Spark context is required
  override def beforeAll(): Unit = {}

  override def beforeEach() {
    
    resetTestEnvironment
    
  }

  // Prevent the Spark Context being recycled
  override def afterEach() {}

  /**
   * Write a row to the table
   */
  test("write a prediction row ") {

    val expectedPortfolioCode = "portfolio code"
    val expectedAtDate = LocalDate.now()
    val expectedEstimatorClass = "Test only"
    val expectedHValue = 10
    val expectedPValue = 95
    val expectedValuation = System.currentTimeMillis()
    instance.persist(expectedPortfolioCode, expectedAtDate, expectedEstimatorClass, expectedHValue, expectedPValue, expectedValuation)
  }
  //
  //
  //
  private def generateContextFileContentValues = {

    dbUrl = "\"jdbc:mysql://localhost:3306/pData\""
    dbDriver = "\"com.mysql.jdbc.Driver\""
    dbUser = "\"root\""
    dbPassword = "\"nbuser\""

  }

  private def generateContextFileContents: String = {

    val predictionsConfig = s"predictions{logDBUrl = ${dbUrl}, logDBDriver = ${dbDriver}, dbUser = ${dbUser}, dbPassword = ${dbPassword}}"
    s"${hadoopAppContextEntry}, ${predictionsConfig}"
  }

  private def generateDefaultInstance = {

    instance = new MySQLPredictionPersistor
  }

  private def generateAppContext {

    val configFile = writeTempFile(generateContextFileContents)
    try {
      val result = ApplicationContext.useConfigFile(configFile)
    } finally {
      configFile.delete()
    }
  }

  private def resetTestEnvironment = {

    generateContextFileContentValues
    generateAppContext
    generateDefaultInstance
  }

}