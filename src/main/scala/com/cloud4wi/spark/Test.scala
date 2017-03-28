package com.cloud4wi.spark

// Test.scala
/* A tiny little testing framework, to
display results and to introduce & promote
unit testing early in the learning curve.
https://github.com/AtomicScala/atomic-scala-examples/blob/master/examples-V1.1/AtomicTest.scala
import com.cloud4wi.spark.Test._
*/

import language.implicitConversions
import java.io.FileWriter

class Test[T](val target:T) {
  val errorLog = "_Test.txt"
  def tst[E](expected:E)(test: => Boolean){
    println(target)
    if(test == false) {
      val msg = "[Error] expected:\n" +
        expected
      println(msg)
      val el= new FileWriter(errorLog,true)
      el.write(target + msg + "\n")
      el.close()
    }
  }
  def str = // Safely convert to a String
    Option(target).getOrElse("").toString
  def is(expected:String) = tst(expected) {
    expected.replaceAll("\r\n","\n") == str
  }
  def is[E](expected:E) = tst(expected) {
    expected == target
  }
  def beginsWith(exp:String) = tst(exp) {
    str.startsWith(
      exp.replaceAll("\r\n","\n"))
  }
}

object Test {
  implicit def any2Atomic[T](target:T) =
    new Test(target)
}