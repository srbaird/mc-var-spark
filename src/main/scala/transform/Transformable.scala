package main.scala.transform

import org.apache.spark.ml.Transformer
import org.apache.spark.sql.DataFrame

/**
 * Implements a series of Transformers
 */
trait Transformable {
  
  def add(t:Transformer):Unit
  
  def transform(d:DataFrame):DataFrame
  
}