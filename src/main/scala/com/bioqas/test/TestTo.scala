package com.bioqas.test

import java.util

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.sql.{Row, DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
  * Created by chailei on 18/7/16.
  */
object TestTo {

//  var a: Int =_
var batchWj: Array[Int] = _


  def main(args: Array[String]) {


    1.0.to(10.0,0.1).foreach(println)

    1.0.to(10.0,0.1).map(_.formatted("%.2f")).foreach(println)

    println(999.0 == 9991)



    //    for(value <- 1.0 to 10.0){
//
//    }
//
//    for(value <- 1 to 1){
//
//    }


//    val ss = SparkSession.builder()
//      .appName("TestTo")
//      .master("local[5]")
//      .getOrCreate()

//    import  ss.implicits._
//
//    val rdd = ss.sparkContext.parallelize(Array(3.0,4.0)).toDF("name").toDF()
//    val result = ss.sparkContext.parallelize(Array(2.5,3.0)).toDF("name").toDF()
//    val rdd1 = ss.sparkContext.parallelize(Array(3.0,4.0)).toDF("name")
//    val result1 = ss.sparkContext.parallelize(Array(2.5,3.0)).toDF("name")
//
//
//    rdd.map(value => value+"1").collect().foreach(println)
//    result.map(value => value+"2").collect().foreach(println)
//
//    rdd1.map(value => value+"1").collect().foreach(println)
//    result1.map(value => value+"2").collect().foreach(println)
//
//
//    Thread.sleep(100000000)

//    val broadcast: Broadcast[Array[Row]] = ss.sparkContext.broadcast(result.collect())
//    import scala.collection.JavaConversions._

//    rdd.foreachPartition(partition =>{
//      partition.foreach(row =>{
////        println(row.getAs[String](0))
//      })
//      println("================")
//      for(value <- broadcast.value){
//        println(value.getAs[Int](0))
//      }
//      println("================")
//
//    })
////
//    val addColwjP2: UserDefinedFunction = ss.udf.register("addcolP2", (str: String) => "0")
//
//    val wjP2Total = rdd.withColumn("DISCARD_NSD", addColwjP2(rdd("name")))
//
//    val addColwjP3: UserDefinedFunction = ss.udf.register("addcolP3", (str: String) => "1")
//
//    val wjP3Total = rdd.withColumn("DISCARD_NSD", addColwjP3(rdd("name")))
//
//    val total = wjP2Total.union(wjP3Total)

//    wjP2Total.show()
//    wjP3Total.show()
//    total.show()


//      val a = 1.0
//      println(a.toInt.toString)


//    var a = null

//    var a = ""

//    a = null
//
//    println(a != null)
//
//    for(value <- 5.0 to 5.0 by 0.1){
//      println(value)
//    }


//    a = 2


//
//    batchWj = Array(3,3,3,3,3)
//
//    batchWj.foreach(println)


//    rdd.foreach(println)



//    rdd.map(line => {
//      val max = line + 1d
//      val min = line -1d
//      (min to max by 0.1).mkString(",")
//
//    }).flatMap(values => values.split(",")).filter(lines => {
//      result
//      true
//    })
//
//    result.filter(line => {
//      rdd.map(line => {
//        val max = line + 1d
//        val min = line -1d
//        (min to max by 0.1).mkString(",")
//
//      }).flatMap(values => values.split(",")).filter(lines => {
//
//        true
//      })
//    }).foreach(println)

//    ss.stop()

  }

}
