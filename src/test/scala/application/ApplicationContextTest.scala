package test.scala.application

import org.junit._
import org.scalatest.junit.AssertionsForJUnit
import org.junit.Assert._
import main.scala.application.ApplicationContext


class ApplicationContextTest extends AssertionsForJUnit {
  

  
  @Before def init() {
    println("Initialize")
  }
  
  
  @Test def getHDSFLocation() { // Uses ScalaTest assertions
    
    assertNotNull(ApplicationContext.getHDSFLocation)

  }
}