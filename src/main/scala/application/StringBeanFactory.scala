package main.scala.application

/**
 * Defines a factory method to create objects from the Spring framework using a string key
 */
trait StringBeanFactory[T] {
  
  def create(beanName:String):T
  
}