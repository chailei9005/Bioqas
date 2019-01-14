package com.bioqas.test

import java.sql.Connection
import java.util.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.jdbc.JdbcDialects

/**
  * Created by chailei on 18/8/22.
  */
object MySQLSpeedApp {

  def main(args: Array[String]) {

    val ss = SparkSession.builder()
      .appName("OracleTestApp")
      .master("local[10]")
      .getOrCreate()
    JdbcDialects.registerDialect(OracleDialectSpeed)

    val tUserDF = ss.read.format("jdbc").options(Map(
    "url" -> "jdbc:oracle:thin:@192.168.8.108:1521:orcl",
    "user" -> "test",
    "password" -> "test",
    "dbtable" -> "TP_E_BD_STATISTICS_RESULT",
    "driver" -> "oracle.jdbc.driver.OracleDriver"
    )).load()

    tUserDF.printSchema()

    val startTime: Long = new Date().getTime
    println("startTime = "+startTime)

    tUserDF.foreachPartition( partition => {

      val connect: Connection = MySQLSpeedUtils.getConnect()


      val pt = connect.prepareStatement(MySQLSpeedUtils.querySql)

//      println("partition")

      val updatePt = connect.prepareStatement(MySQLSpeedUtils.updateSql)

      val pstmt = connect.prepareStatement(MySQLSpeedUtils.insertSql)

      connect.setAutoCommit(false)
      partition.foreach( value => {

//        println("foreach")
        val pk_id = value.getAs[String](0)

        if(MySQLSpeedUtils.queryRecord(pt,pk_id)){

          MySQLSpeedUtils.updateBatch(updatePt,value,pk_id)

        }else{

          //TODO groupid grouptype
          MySQLSpeedUtils.insertBatch(pstmt,value,pk_id,4)

        }

      })

      updatePt.executeBatch()
      pstmt.executeBatch()

      connect.commit()
      MySQLSpeedUtils.close(connect,pstmt)
      MySQLSpeedUtils.close(connect,updatePt)
      MySQLSpeedUtils.close(connect,pt)
    })

    val endTime: Long = new Date().getTime
    println("endTime = "+endTime)

    println("time = " + (endTime - startTime)/1000)

    Thread.sleep(10000000)

    ss.stop()


  }

}
