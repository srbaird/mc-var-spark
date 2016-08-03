package main.scala.application

import java.time.LocalDate
import java.io.File

trait StandardArguments {
  
    def validateArgs(args: Array[String]): (String, String, LocalDate) = {

    if (args.length < 3) {
      throw new IllegalArgumentException(s"Expected 2 arguments, got ${args.length}")
    }
    val file = new File(args(0))
    if (!file.exists()) {
      throw new IllegalArgumentException(s"${args(0)} is not a file")
    }
    val date = LocalDate.parse(args(2))
    (args(0), args(1), date)
  }
}