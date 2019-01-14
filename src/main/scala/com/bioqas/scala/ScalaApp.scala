package com.bioqas.scala

import scala.collection.mutable

/**
  * Created by chailei on 18/8/6.
  */
object ScalaApp {

  def main(args: Array[String]) {

    say()
    say(name="chailei",value = "baixue")

    val sss = 3
    val ddd = 4

    s"${sss}"

    println(null)
    println("")

    var set = mutable.HashSet("chaiilei","baixue","baibai")

    for(x: String <- set.mkString(",").split(",").toSet){
      println(x)
    }


  }


  // 默认参数，括号不能省略
  def say(value:String = "chailei"): Unit ={
    println(value)
  }

  def say(value: String,name: String): Unit ={
    println(value)
    println(name)
  }

}
