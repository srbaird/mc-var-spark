package main.scala.application

import org.springframework.context.support.GenericApplicationContext
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader
import org.apache.hadoop.fs.FileSystem
import org.springframework.core.io.InputStreamResource
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger

trait SpringContextFromHDFS {

  //
  private val logger = Logger.getLogger(getClass)
  
  def loadContext(location: String): GenericApplicationContext = {

    logger.debug(s"Load the Spring Framework from '${location}'")
    // Generate the application context
    val ctx = new GenericApplicationContext();
    val xmlReader = new XmlBeanDefinitionReader(ctx);
    xmlReader.setValidationMode(XmlBeanDefinitionReader.VALIDATION_XSD) // Explicitly set to enable use of InputStreamResource
    val fs = FileSystem.get(ApplicationContext.getHadoopConfig)
    val is = fs.open(new Path(location))
    xmlReader.loadBeanDefinitions(new InputStreamResource(is));
    ctx.refresh();
    ctx
  }
}