package main.scala.models

import main.scala.application.StringBeanFactory
import org.apache.spark.ml.util.MLReadable
import main.scala.application.ApplicationContext
import org.apache.log4j.Logger

class MLReadableStringBeanFactory(mapping: Map[String, String]) extends StringBeanFactory[MLReadable[_]] {

  def this() = this(Map[String, String]())
  //
  //
  //
  private val logger = Logger.getLogger(getClass)

  override def create(beanName: String): MLReadable[_] = {

    if (beanName == null) {
      throw new IllegalArgumentException(s"Invalid key supplied: ${beanName}")
    }
    if (!mapping.contains(beanName)) {
      throw new IllegalStateException(s"Supplied key was not found: ${beanName}")
    }
    logger.trace(s"Load a loadable model from '${beanName}'. ")
    ApplicationContext.springApplicationContext.getBean(mapping(beanName)).asInstanceOf[GetModel[MLReadable[_]]].get
  }
}