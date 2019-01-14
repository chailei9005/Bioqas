package com.bioqas.test

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.spark.sql.Row

/**
  * Created by chailei on 18/7/24.
  */
object OracleUtilsGroup {

  val driver = "oracle.jdbc.driver.OracleDriver"
  val url = "jdbc:oracle:thin:@192.168.8.108:1521:orcl"
  val username = "test"
  val password = "test"

  val insertSql = "insert into TEST.TP_E_BD_STATISTICS_RESULT(" +
    "CENTER_ID,PLAN_ID,SPEC_ID,TIMES_ID," +
    "ITEM_ID,BATCH_ID," +
    "GROUPID,TARGET,TOTAL,QUALIFIED_RATE," +
    "UNQUALIFIED_RATE) values(" +
    "?,?,?,?,?," +
    "?,?,?,?,?)"

//  val updateSql = "update TEST.TP_E_BD_STATISTICS_RESULT set STATISTICS_COUNT=?," +
//    "STATISTICS_AVG=?,STATISTICS_MEDIAN=?,STATISTICS_SD=?,STATISTICS_CV=?," +
//    "STATISTICS_MAX=?,STATISTICS_MIN=? where ID=?"
//
//  val querySql = "select * from TEST.TP_E_BD_STATISTICS_RESULT where ID = ?"

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

    pstmt.setString(1,value.getAs[String](15))
    pstmt.setInt(2,value.getAs[Integer](0))
    pstmt.setInt(3,value.getAs[Integer](1))
    pstmt.setInt(4,value.getAs[Integer](2))
    pstmt.setInt(5,value.getAs[Integer](4))
    pstmt.setInt(6,value.getAs[Integer](3))
    pstmt.setInt(7,value.getAs[Integer](5))


    pstmt.setLong(8,value.getAs[Long](6))
    pstmt.setDouble(9,value.getAs[Double](7))
    pstmt.setDouble(10,value.getAs[Double](8))
    pstmt.setDouble(11,value.getAs[Double](9))
    pstmt.setDouble(12,value.getAs[Double](10))
    pstmt.setDouble(13,value.getAs[Double](11))
    pstmt.setDouble(14,value.getAs[Double](12))

    // 操作失误  14 写成了 10 导致报错    索引中丢失 IN 或 OUT 参数:: 14
    pstmt.setInt(15,value.getAs[Integer](13))

    if(group_type != 0){
//      println(value.getAs[Integer](13))
      if("".equals(value.getAs[Integer](14)) || value.getAs[Integer](14) != null){
        pstmt.setInt(16,value.getAs[Integer](14))
      }else{
        pstmt.setInt(16,-1)
      }
//      pstmt.setInt(16,999)

    }else{
      pstmt.setInt(16,0)
    }
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
