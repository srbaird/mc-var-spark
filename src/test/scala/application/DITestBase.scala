package test.scala.application

import main.scala.transform.DoublesOnlyTransformer
import org.springframework.context.support.GenericApplicationContext
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader
import org.apache.spark.sql.SQLContext
import java.io.File
import main.scala.application.ApplicationContext
import org.springframework.core.io.UrlResource
import java.net.URL

class DITestBase extends SparkTestBase {

  var ctx: GenericApplicationContext = _
  val springContextFileName = "resources/test/application-context.xml"

  override def beforeAll(): Unit = {

    super.beforeAll
  }

  override def afterAll(): Unit = {

    super.afterAll
  }

  override def beforeEach() {

    super.beforeEach

  }

  override def afterEach() {

    super.afterEach
  }

  //
  //
  //
  def generateApplicationContext = {

    ctx = new GenericApplicationContext();
    val xmlReader = new XmlBeanDefinitionReader(ctx);
    xmlReader.loadBeanDefinitions(new UrlResource(new URL("file", "", springContextFileName)));
    ctx.refresh();
    val applicationContextFileNameBeanName = "applicationContextFileName"
    val applicationContextFileName = ctx.getBean(applicationContextFileNameBeanName).asInstanceOf[String]
    // Set the Application Context values
    val contextFile = new File(applicationContextFileName)
    ApplicationContext.useConfigFile(contextFile)
    ApplicationContext.useHadoopConfig(contextFile)
    ApplicationContext.sc(sc)
  }

}