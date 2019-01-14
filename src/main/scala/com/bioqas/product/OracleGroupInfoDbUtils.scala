package com.bioqas.product

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.spark.sql.Row

/**
  * Created by chailei on 18/7/24.
  */
object OracleGroupInfoDbUtils {

  val driver = "oracle.jdbc.driver.OracleDriver"
//  val url = "jdbc:oracle:thin:@192.168.8.108:1521:orcl"
  val url = "jdbc:oracle:thin:@47.92.136.8:1521:biodev"
//  val username = "test"
  val username = "biodev"
//  val password = "test"
  val password = "Bio#2018#Ora"
  val database = "biodev"

  val insertSql = s"insert into ${database}.TP_E_BD_TARGET_PEC_LAB(" +
    "ID,CENTER_ID,PLAN_ID,SPEC_ID,TIMES_ID," +
    "ITEM_ID,BATCH_ID,GROUP_ID,TARGET," +
    "QUALIFIED_PERCENT,UNQUALIFIED_PERCENT) values(" +
    "?,?,?,?,?," +
    "?,?,?,?,?," +
    "?)"

  val updateSql = s"update ${database}.TP_E_BD_TARGET_PEC_LAB set TARGET=?," +
    "QUALIFIED_PERCENT=?,UNQUALIFIED_PERCENT=? where ID=?"

  val querySql = s"select * from ${database}.TP_E_BD_TARGET_PEC_LAB where ID = ?"

  def getConnect() : Connection = {

    Class.forName(driver)

    val connection: Connection = DriverManager.getConnection(url, username, password)

    connection
  }

  def close(connection: Connection,pstmt :PreparedStatement) : Unit = {

    if(pstmt != null){
      try{
        pstmt.close()
      }catch {
        case e:Exception => println("pstmt close failed")
      }
    }
    if(connection != null){
      try{
        connection.close()
      }catch {
        case e:Exception => println("connection close failed")
      }
    }

  }

  def insertBatch(pstmt: PreparedStatement,value:Row,pk_id: String,group_type: Int): Unit ={

//    pstmt.setString(1,value.getAs[String](15))
    pstmt.setInt(1,value.getAs[String](0).toInt)
    pstmt.setInt(2,value.getAs[String](1).toInt)
    pstmt.setInt(3,value.getAs[String](2).toInt)
    pstmt.setInt(4,value.getAs[String](4).toInt)
    pstmt.setInt(5,value.getAs[String](3).toInt)
    pstmt.setInt(6,value.getAs[String](5).toInt)


    pstmt.setLong(8,value.getAs[String](6).toInt)
    pstmt.setDouble(9,value.getAs[Double](7))
    pstmt.setDouble(10,value.getAs[Double](8))
    pstmt.setDouble(11,value.getAs[Double](9))
//    pstmt.setDouble(12,value.getAs[Double](10))
//    pstmt.setDouble(13,value.getAs[Double](11))
//    pstmt.setDouble(14,value.getAs[Double](12))

    // 操作失误  14 写成了 10 导致报错    索引中丢失 IN 或 OUT 参数:: 14
//    pstmt.setInt(15,value.getAs[Integer](13))

//    if(group_type != 0){
////      println(value.getAs[Integer](13))
//      if("".equals(value.getAs[Integer](14)) || value.getAs[Integer](14) != null){
//        pstmt.setInt(16,value.getAs[Integer](14))
//      }else{
//        pstmt.setInt(16,-1)
//      }
////      pstmt.setInt(16,999)
//
//    }else{
//      pstmt.setInt(16,0)
//    }
//
//    pstmt.setInt(17,value.getAs[Integer](16))
//    println("insert")
    pstmt.addBatch()
  }


  def updateBatch(pstmt: PreparedStatement,value: Row,pk_id: String): Unit ={
    pstmt.setLong(1,value.getAs[Long](6))
    pstmt.setDouble(2,value.getAs[Double](7))
    pstmt.setDouble(3,value.getAs[Double](8))
    pstmt.setDouble(4,value.getAs[Double](9))
    pstmt.setDouble(5,value.getAs[Double](10))
    pstmt.setDouble(6,value.getAs[Double](11))
    pstmt.setDouble(7,value.getAs[Double](12))
    pstmt.setString(8,value.getAs[String](15))
//    println("update")
    pstmt.addBatch()
  }

  def queryRecord(pstmt: PreparedStatement,pk_id: String)={
    pstmt.setString(1,pk_id)
    val query: ResultSet = pstmt.executeQuery()
    query.next()
  }



  def main(args: Array[String]) {


    println(getConnect())
    val aa = 11f
    11.getClass.getName

    println(aa.isInstanceOf[Int])
  }

}
