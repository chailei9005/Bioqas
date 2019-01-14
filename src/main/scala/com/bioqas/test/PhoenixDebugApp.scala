package com.bioqas.test

import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.sql.SparkSession
import org.apache.phoenix.spark._

// phoenixTableAsDataFrame
import org.apache.spark.sql.functions._
/**
  * Created by chailei on 18/6/29.
  */
object PhoenixDebugApp {

  def main(args: Array[String]) {


    val ss = SparkSession.builder()
      .appName("OracleTestApp")
      .master("local[2]")
      .getOrCreate()

//    val sqlTable = "(select * from TP_E_BD_REPORT_DATE_BAK where PLAN_ID=168 AND SPEC_ID=57) as a"
//
//    val phoenixDF = ss.read.format("org.apache.phoenix.spark")
////      .option("table" , "TP_E_BD_REPORT_DATE_BAK")
//        .option("table" , "sqlTable")
//      .option( "zkUrl" , "biaoyuan01:2181")
//      .load()

//    val phoenixDF = ss.read.format("org.apache.phoenix.spark").options(Map(
//      "zkUrl" -> "biaoyuan01:2181",
//      "table" -> "(select * from TP_E_BD_REPORT_DATE_BAK where PLAN_ID=168 AND SPEC_ID=57) a"
//    )).load()

    val id = "47"
    val biaoshi = "%"
    println(s"ID like \'${biaoshi}${id}\'")
    val phoenixDF = ss.sqlContext.phoenixTableAsDataFrame(
      "TP_E_BD_REPORT_DATE",    //TP_E_BD_REPORT_DATE_BUCKET
      Seq("ID","CENTER_ID","PLAN_ID","REPORT_DATA"),// 字段必须是大写
      Some(s"ID like \'${biaoshi}${id}${biaoshi}\' or ID like \'${biaoshi}b8${biaoshi}\'"),//Some(s"ID like \'${biaoshi}${id}\'"),
      Some("biaoyuan01:2181")
    )

    phoenixDF.printSchema()

    phoenixDF.createOrReplaceTempView("tp")
//    phoenixDF.createGlobalTempView("tp")

    // 建表的时候REPORT_DATA是varchar类型，做聚合操作的时候，貌似是按ASCII排序的
//    ss.sql("select max(REPORT_DATA),min(REPORT_DATA),avg(REPORT_DATA) from tp").show()

//    phoenixDF.select("ID","REPORT_DATA").groupBy("ID").agg(count("ID")).show()

    phoenixDF.select("CENTER_ID","PLAN_ID","REPORT_DATA")
      .groupBy("CENTER_ID","PLAN_ID").agg(max("REPORT_DATA"),min("REPORT_DATA")).show()

    phoenixDF.show(90)

    val isNUll :String = null
    println(isNUll == null)

    val md = DigestUtils.md5Hex("chailei")
    println("md = " + md)
    val md1 = DigestUtils.md5Hex("chailei0")
    println("md = " + md1)
    phoenixDF.show(20,false)

//    ss.sql("select * from TP_E_BD_REPORT_DATE_BAK where PLAN_ID=168 AND SPEC_ID=57")
//      .show(20,false)

//    println("count = " + phoenixDF.count())

    val d = 999d
    println(d)
    println(d == 999d)
    import ss.implicits._
    val df = ss.sparkContext.parallelize(Seq(("a", 1.0), ("a", 2.0), ("b", 2.0), ("b", 3.9), ("c", 1.8))).toDF("id", "num")

    df.filter("num!=2d").show()

    val df1 = ss.sparkContext.parallelize(Seq(5)).toDF("id")

    import ss.implicits._

    df1.printSchema()

    val df2 = df1.groupBy("id").agg(mean("id"),stddev("id"),stddev("id")/mean("id"))

    val a = 1



    df2.select("id","sd").show()


    ss.stop()

  }

}
