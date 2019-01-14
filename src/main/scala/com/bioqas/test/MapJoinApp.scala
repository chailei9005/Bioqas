package com.bioqas.test

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by chailei on 18/8/21.
  */
object MapJoinApp {

  def main(args: Array[String]) {

    val ss = SparkSession.builder()
      .master("local[2]")
      .appName("MapJoinApp")
        .config("spark.sql.planner.skewJoin.threshold","true")
      .getOrCreate()

    import ss.implicits._

    val person = ss.sparkContext.parallelize(Array("1,chailei","2,baixue"))

    val info = ss.sparkContext.parallelize(Array("1,1850008","2,1860069","3,leilei"))




//    person.foreach(println)

    val personDF: DataFrame = person.map(line => {
      (line.split(",")(0),line.split(",")(1))
    }).toDF("id","name")

    val infoDF: DataFrame = info.map(line => {
      (line.split(",")(0),line.split(",")(1))
    }).toDF("id","phone")

    val broadcast: Broadcast[DataFrame] = ss.sparkContext.broadcast(infoDF)

    val resultDF: DataFrame = personDF.join(broadcast.value,"id")

    resultDF.foreachPartition( partiton => {

      partiton.foreach(println)
    })

//
//    resultDF.show()

//    val array = ss.sparkContext.parallelize(Array("1","2","3","4"))
//
//    val f = array.map(_*2).toDF("value")
//
////    f.show()
//
//    f.foreachPartition( partiton => {
//
//      partiton.foreach(println)
//    })

    Thread.sleep(10000000)

    ss.stop()
  }

}
