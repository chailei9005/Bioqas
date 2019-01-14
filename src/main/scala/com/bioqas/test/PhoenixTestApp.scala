package com.bioqas.test

import java.sql.ResultSet

import com.bioqas.utils.PhoenixUtils
import org.apache.spark.sql.SparkSession
import org.apache.phoenix.spark._
/**
  * Created by chailei on 18/8/17.
  */
object PhoenixTestApp {

  def main(args: Array[String]) {


    val ss = SparkSession.builder()
      .master("local[2]")
      .appName("PhoenixTestApp")
      .getOrCreate()


//    val phoenix = new PhoenixUtils()
//
//    val resultSet: ResultSet = phoenix.searchFromHbase(
//      "select * from TP_E_BD_REPORT_DATE_BUCKET")
//
//    while(resultSet.next()){
//
//      println(resultSet.getString(1))
//
//    }

//    val phoenixDF = ss.sqlContext.phoenixTableAsDataFrame(
//      "TP_E_BD_REPORT_DATE_BUCKET",    //TP_E_BD_REPORT_DATE_BUCKET
//      Seq("ID","CENTER_ID","PLAN_ID","REPORT_DATA"),// 字段必须是大写
//      Some("biaoyuan01:2181/hbase")
//    )
//
//    phoenixDF.show()

      val phoenixDF = ss.read.format("org.apache.phoenix.spark")
        .option("table" , "TP_E_BD_REPORT_DATE_BUCKET")
        .option( "zkUrl" , "biaoyuan01:2181")
        .load()


    phoenixDF.show(1000)




    ss.stop()
  }

}
