package test.scala.application

import main.scala.application.ApplicationContext

class HDFSConfigTest extends DITestBase {

  override def beforeAll(): Unit = {

    super.beforeAll
  }

  override def beforeEach() {
    super.generateApplicationContext
  }

  // Overridden to prevent Spark Context from being recycled
  override def afterEach() {}

  test("test hdfs set-up") {
    println(s"${System.getenv("SPARK_HOME")}")
    println(s"${ApplicationContext.getHadoopConfig.get("fs.default.name")}")
  }
}