package com.bioqas.scala

/**
  * Created by chailei on 18/7/24.
  */
class Person(val name:String,val age:Int) {

  //val weight : Int = 111
}

class Girl(name : String,age : Int) extends Person(name,age){

  //override var weight = 150
}
