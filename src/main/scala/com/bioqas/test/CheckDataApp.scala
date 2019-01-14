package com.bioqas.test

import org.apache.spark.sql.{DataFrame, SparkSession}
/**
  * Created by chailei on 18/11/29.
  */
object CheckDataApp {

  def main(args: Array[String]) {


    val ss = SparkSession.builder()
      .appName("CheckDataApp")
      .master("local[2]")
      .getOrCreate()



    val data: DataFrame = ss.read.format("csv")
    .option("header", "true")
    .option("inferSchema",true.toString)
    .load("/Users/chailei/antu/checkData.csv")

    data.printSchema()
    import ss.implicits._


    val dF: DataFrame = data.rdd.map(value => {
      (value.getAs[String](0), value.getAs[Double](5))
    }).toDF("lab", "score")


    dF.createOrReplaceTempView("data")


    val sql: DataFrame = ss.sql("select mean(score),stddev(score),3*stddev(score) from data")

    sql.createOrReplaceTempView("data1")





    ss.stop()

  }

}
