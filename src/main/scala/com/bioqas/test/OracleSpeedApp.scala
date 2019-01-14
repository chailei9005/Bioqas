package com.bioqas.test

import java.sql.Connection
import java.util.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.jdbc.JdbcDialects

/**
  * Created by chailei on 18/8/22.
  */
object OracleSpeedApp {

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

    tUserDF.repartition(10).foreachPartition( partition => {

      val connect: Connection = OracleSpeedUtils.getConnect()


      val pt = connect.prepareStatement(OracleSpeedUtils.querySql)

//      println("partition")

      val updatePt = connect.prepareStatement(OracleSpeedUtils.updateSql)

      val pstmt = connect.prepareStatement(OracleSpeedUtils.insertSql)

      connect.setAutoCommit(false)
      partition.foreach( value => {

//        println("foreach")
        val pk_id = value.getAs[String](0)

        if(OracleSpeedUtils.queryRecord(pt,pk_id)){

          OracleSpeedUtils.updateBatch(updatePt,value,pk_id)

        }else{

          //TODO groupid grouptype
          OracleSpeedUtils.insertBatch(pstmt,value,pk_id,4)

        }

      })

      updatePt.executeBatch()
      pstmt.executeBatch()

      connect.commit()
      OracleSpeedUtils.close(connect,pstmt)
      OracleSpeedUtils.close(connect,updatePt)
      OracleSpeedUtils.close(connect,pt)
    })

    val endTime: Long = new Date().getTime
    println("endTime = "+endTime)

    println("time = " + (endTime - startTime))

    ss.stop()


  }

}
