package com.bioqas.test

import java.io.Serializable
import java.sql.Connection
import java.util.Date

import com.bioqas.utils.{PhoenixUtils, PhoenixJsonParseSpeedUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.jdbc.JdbcDialects

/**
  * Created by chailei on 18/8/22.
  */
object PhoenixSpeedApp {


  def main(args: Array[String]) {

    val ss = SparkSession.builder()
      .appName("OracleTestApp")
      .master("local[16]")
      .getOrCreate()
    JdbcDialects.registerDialect(OracleDialectSpeed)

    val tUserDF = ss.read.format("jdbc").options(Map(
      "url" -> "jdbc:oracle:thin:@192.168.8.108:1521:orcl",
      "user" -> "test",
      "password" -> "test",
      "dbtable" -> "TP_E_BD_STATISTICS_RESULT",
      "driver" -> "oracle.jdbc.driver.OracleDriver"
    )).load()

    val startTime: Long = new Date().getTime
    println("startTime = "+startTime)


    tUserDF.repartition(15).foreachPartition( partition => {

      val phoenixUtil = new PhoenixUtils()
      var batchSize = 0
      val commitSize = 500
      partition.foreach( value => {


        val sql = PhoenixJsonParseSpeedUtil.parseJSONToPhoenixSQL(value)
        phoenixUtil.saveToHbase(sql)

        batchSize = batchSize + 1

        if (batchSize % commitSize == 0) {
          phoenixUtil.conn.commit()
        }


      })

      phoenixUtil.closeConn()

    })

    val endTime: Long = new Date().getTime
    println("endTime = "+endTime)

    println("time = " + (endTime - startTime)/1000)

    Thread.sleep(10000000)


    ss.stop()

  }
}
