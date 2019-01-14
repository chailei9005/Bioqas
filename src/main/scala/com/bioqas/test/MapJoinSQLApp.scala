package com.bioqas.test

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by chailei on 18/8/21.
  */
object MapJoinSQLApp {

  def main(args: Array[String]) {

    val ss = SparkSession.builder()
      .master("local[2]")
      .appName("MapJoinApp")
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


    personDF.createOrReplaceTempView("person")

    infoDF.createOrReplaceTempView("info")

    val resultDF: DataFrame = ss.sql("select * from person join info where person.id = info.id")

//    val resultDF: DataFrame = infoDF.join(broadcast.value,"id")

    resultDF.foreachPartition( partiton => {

      partiton.foreach(println)
    })


    Thread.sleep(10000000)

    ss.stop()
  }

}
