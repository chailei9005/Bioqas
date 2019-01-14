package com.bioqas.test

import org.apache.spark.sql.SparkSession

/**
  * Created by chailei on 18/6/11.
  */
object OracleTestApp {

  def main(args: Array[String]) {


    val ss = SparkSession.builder()
      .appName("OracleTestApp")
      .master("local[2]")
      .getOrCreate()

    val id = "id = 786 or id = 787"
//  "dbtable" -> s"(select * from TP_E_BD_REPORT_DATE where $id ) a",

    val tUserDF = ss.read.format("jdbc").options(Map(
      "url" -> "jdbc:oracle:thin:@192.168.8.108:1521:orcl",
      "user" -> "bioqas",
      "password" -> "bioqas",
      "dbtable" -> "TP_E_BD_REPORT_DATE",
      "driver" -> "oracle.jdbc.driver.OracleDriver"
    )).load()

    tUserDF.printSchema()

    tUserDF.select("*").take(10).foreach(println)


    ss.stop()

  }

}
