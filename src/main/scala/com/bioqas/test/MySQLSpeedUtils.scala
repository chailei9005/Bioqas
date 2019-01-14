package com.bioqas.test

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.spark.sql.Row

/**
  * Created by chailei on 18/7/24.
  */
object MySQLSpeedUtils {

  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://localhost:3306/bioqas?useSSL=true"
//  val url = "jdbc:oracle:thin:@47.92.136.8:1521:biodev"
  val username = "root"
  val password = "chlhyj"
  val database = "bioqas"

  val insertSql = s"insert into ${database}.INSERT_SPEED(" +
    "ID,CENTER_ID,PLAN_ID,SPEC_ID,TIMES_ID," +
    "ITEM_ID,BATCH_ID,STATISTICS_COUNT," +
    "STATISTICS_AVG,STATISTICS_MEDIAN," +
    "STATISTICS_SD,STATISTICS_CV," +
    "STATISTICS_MAX,STATISTICS_MIN," +
    "GROUP_TYPE,GROUPID) values(" +
    "?,?,?,?,?," +
    "?,?,?,?,?," +
    "?,?,?,?,?,?)"

  val updateSql = s"update ${database}.INSERT_SPEED set STATISTICS_COUNT=?," +
    "STATISTICS_AVG=?,STATISTICS_MEDIAN=?,STATISTICS_SD=?,STATISTICS_CV=?," +
    "STATISTICS_MAX=?,STATISTICS_MIN=? where ID=?"

  val querySql = s"select * from ${database}.INSERT_SPEED where ID = ?"

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

    pstmt.setString(1,value.getAs[String](0))
    pstmt.setInt(2,value.getAs[Integer](1))
    pstmt.setInt(3,value.getAs[Integer](1))
    pstmt.setInt(4,value.getAs[Integer](2))
    pstmt.setInt(5,value.getAs[Integer](4))
    pstmt.setInt(6,value.getAs[Integer](3))
    pstmt.setInt(7,value.getAs[Integer](5))


    pstmt.setInt(8,value.getAs[Integer](6))
    pstmt.setInt(9,value.getAs[Integer](7))
    pstmt.setInt(10,value.getAs[Integer](8))
    pstmt.setInt(11,value.getAs[Integer](9))
    pstmt.setInt(12,value.getAs[Integer](10))
    pstmt.setInt(13,value.getAs[Integer](11))
    pstmt.setInt(14,value.getAs[Integer](12))

    // 操作失误  14 写成了 10 导致报错    索引中丢失 IN 或 OUT 参数:: 14
    pstmt.setInt(15,value.getAs[Integer](13))

//    if(group_type != 0){
//      println(value.getAs[Integer](13))
//      if("".equals(value.getAs[Integer](14)) || value.getAs[Integer](14) != null){
        pstmt.setInt(16,value.getAs[Integer](14))
//      }else{
//        pstmt.setInt(16,-1)
//      }
//      pstmt.setInt(16,999)

//    }else{
//      pstmt.setInt(16,0)
//    }
//    println("insert")
    pstmt.addBatch()
  }


  def updateBatch(pstmt: PreparedStatement,value: Row,pk_id: String): Unit ={
    pstmt.setInt(1,value.getAs[Integer](6))
    pstmt.setInt(2,value.getAs[Integer](7))
    pstmt.setInt(3,value.getAs[Integer](8))
    pstmt.setInt(4,value.getAs[Integer](9))
    pstmt.setInt(5,value.getAs[Integer](10))
    pstmt.setInt(6,value.getAs[Integer](11))
    pstmt.setInt(7,value.getAs[Integer](12))
    pstmt.setInt(8,value.getAs[Integer](15))
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
  }

}
