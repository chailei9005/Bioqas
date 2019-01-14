package com.bioqas.test

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.SparkSession

/**
  * Created by chailei on 18/6/7.
  */
object StatApp {

  def main(args: Array[String]) {


    val ss = SparkSession.builder()
      .appName("Stat")
      .master("local[3]")
      .getOrCreate()

    val lines = ss.sparkContext.textFile("/Users/chailei/antu/bioqas/stattest.txt")

    val obser = lines.map(_.split(",")).map(p => Vectors.dense(p(1).toDouble,
      p(2).toDouble,p(3).toDouble,p(4).toDouble))

    val summer = Statistics.colStats(obser)

    println("最大值："+summer.max)
    println("最小值"+summer.min)
    println("方差"+summer.variance)
    println("均值"+summer.mean)




    lines.foreach(println)




    ss.stop()



  }

}
