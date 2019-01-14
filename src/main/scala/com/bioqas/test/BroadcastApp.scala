package com.bioqas.test

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by chailei on 18/8/7.
  */
object BroadcastApp {

  def main(args: Array[String]) {


    val ss = SparkSession.builder()
      .appName("BroadcastApp")
      .master("local[2]")
      .getOrCreate()

    val user = ss.sparkContext.parallelize(List(
      "chailei 18", "baixue 18"))

    val info = ss.sparkContext.parallelize(List(
      "chailei f","baixue m","baichai f"))

    import ss.implicits._
    val userDF = user.map(value => {
      val split: Array[String] = value.split(" ")
      (split(0),split(1))
    }).toDF("name","age")

    val infoDF = info.map(value => {
      val split: Array[String] = value.split(" ")
      (split(0),split(1))
    }).toDF("name","sex")

//    val broadcast: Broadcast[DataFrame] = ss.sparkContext.broadcast(infoDF)

//    val dataFrame: DataFrame = userDF.join(broadcast.value,userDF("name")===infoDF("name"))

    userDF.createOrReplaceTempView("userDF")
    userDF.createOrReplaceTempView("infoDF")

    ss.sql("cache table userDF_t as select * from userDF")

    ss.sql("select * from infoDF i join userDF_t a on i.name=a.name").show()

//    dataFrame.show()

    Thread.sleep(1000000)
    ss.stop()


  }

}
