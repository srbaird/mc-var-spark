package test.scala.application

import org.springframework.context.support.ClassPathXmlApplicationContext
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader
import org.springframework.context.support.GenericApplicationContext
import org.springframework.core.io.UrlResource
import java.net.URL
import main.scala.application.ApplicationContext
import java.io.File

class ContextTestBase extends SparkTestBase {

  var instance: GenericApplicationContext = _

  val springContextFileName = "resources/test/application-context.xml"

  // Override until a Spark context is required
  override def beforeAll(): Unit = {}

  override def beforeEach() {

    instance = new GenericApplicationContext();
    val xmlReader = new XmlBeanDefinitionReader(instance);
    xmlReader.loadBeanDefinitions(new UrlResource(new URL("file", "", springContextFileName)));
  }

  // Prevent the Spark Context being recycled
  override def afterEach() {}

  /**
   * Test reading the name of the application context file
   */
  test("test reading the name of the application context file") {

    instance.refresh();
    val expectedBeanName = "applicationContextFileName"
    val expectedApplicationContextFileName = "resources/test/applicationContext"
    val resultApplicationContextFileName = instance.getBean(expectedBeanName)

    assert(expectedApplicationContextFileName == resultApplicationContextFileName)
  }

  /**
   * Test building the ApplicationContext from the application context fileName
   */
  test("test building the ApplicationContext from the application context fileName") {

    instance.refresh();
    val expectedBeanName = "applicationContextFileName"
    val resultApplicationContextFileName = instance.getBean(expectedBeanName).asInstanceOf[String]
    ApplicationContext.useConfigFile(new File(resultApplicationContextFileName))
  }
}