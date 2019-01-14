package com.bioqas.test

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.spark.sql.Row

/**
  * Created by chailei on 18/7/24.
  */
object PhoenixSpeedUtils {

  // 47.96.191.250 118.31.9.251 47.97.180.54(
  //  val connstr = "jdbc:phoenix:biaoyuan01,biaoyuan02,biaoyuan03:2181/hbase"

  val connstr = "jdbc:phoenix:biaoyuan01,biaoyuan02,biaoyuan03:2181/hbase"
  val conn = DriverManager.getConnection(connstr)

  conn.setAutoCommit(false)

  var pstmt : PreparedStatement = null

  var rs : ResultSet = null


  def saveToHbase(sql :String){

    try{
      pstmt = conn.prepareStatement(sql)
      pstmt.executeUpdate()
    } catch {
      case e: Exception =>
        println(e.getMessage)
    }

  }

  def searchFromHbase(sql:String): ResultSet = {


    pstmt = conn.prepareStatement(sql)

    rs = pstmt.executeQuery()

    rs
  }


  def closeConn(): Unit ={

    try{
      if(conn != null){
        conn.commit()
        conn.close()
      }
      if(pstmt != null){
        pstmt.close()
      }
    }catch {
      case e: Exception => println(e.getMessage)
    }

  }


}
