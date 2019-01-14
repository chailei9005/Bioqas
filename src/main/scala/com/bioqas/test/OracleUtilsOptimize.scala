package com.bioqas.test

import java.sql._

import com.bioqas.utils.Constants
import org.apache.spark.sql.Row

/**
  * Created by chailei on 18/7/24.
  */
object OracleUtilsOptimize {

  val driver = "oracle.jdbc.driver.OracleDriver"
//  val url = "jdbc:oracle:thin:@192.168.8.108:1521:orcl"
  val url = "jdbc:oracle:thin:@47.92.136.8:1521:biodev"
//  val username = "test"
  val username = "biodev"
//  val password = "test"
  val password = "Bio#2018#Ora"
    val database = "ZPGLXT"



  val sql = s"MERGE INTO ${Configs.getString(Constants.DB_DATABASE)}.TP_E_BD_STATISTICS_RESULT t1 USING (SELECT ? ID ,? CENTER_ID, ? PLAN_ID, ? SPEC_ID,? TIMES_ID," +
    "? ITEM_ID,? BATCH_ID,? STATISTICS_COUNT,? STATISTICS_MEDIAN,? STATISTICS_AVG,? STATISTICS_SD," +
    "? STATISTICS_CV,? STATISTICS_MAX,? STATISTICS_MIN,? GROUP_TYPE,? GROUPID,? DISCARD_NSD FROM dual) t2 ON " +
    "(t1.ID = t2.ID) WHEN MATCHED THEN UPDATE SET t1.STATISTICS_COUNT = t2.STATISTICS_COUNT,t1.STATISTICS_MEDIAN = t2.STATISTICS_MEDIAN " +
    ",t1.STATISTICS_AVG = t2.STATISTICS_AVG,t1.STATISTICS_SD = t2.STATISTICS_SD,t1.STATISTICS_CV = t2.STATISTICS_CV " +
    ",t1.STATISTICS_MAX = t2.STATISTICS_MAX,t1.STATISTICS_MIN = t2.STATISTICS_MIN WHEN NOT " +
    "MATCHED THEN INSERT (ID,CENTER_ID,PLAN_ID,SPEC_ID,TIMES_ID,ITEM_ID,BATCH_ID,STATISTICS_COUNT," +
    "STATISTICS_MEDIAN,STATISTICS_AVG,STATISTICS_SD,STATISTICS_CV,STATISTICS_MAX,STATISTICS_MIN,GROUP_TYPE," +
    "GROUPID,DISCARD_NSD) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

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

  def insertBatch(pstmt: PreparedStatement,value:Row): Unit ={

    pstmt.setString(1,value.getAs[String](15))
    pstmt.setInt(2,value.getAs[Integer](0))
    pstmt.setInt(3,value.getAs[Integer](1))
    pstmt.setInt(4,value.getAs[Integer](2))
    pstmt.setInt(5,value.getAs[Integer](4))
    pstmt.setInt(6,value.getAs[Integer](3))
    pstmt.setInt(7,value.getAs[Integer](5))


    pstmt.setLong(8,value.getAs[Long](6))
    if(value.getAs[Double](7) == 99999){

      pstmt.setNull(9,Types.DOUBLE)
      pstmt.setNull(12,Types.DOUBLE)
    }else{
      pstmt.setDouble(9,value.getAs[Double](7))
      pstmt.setDouble(12,value.getAs[Double](10))
    }
    pstmt.setDouble(10,value.getAs[Double](8))
    pstmt.setDouble(11,value.getAs[Double](9))
    pstmt.setDouble(13,value.getAs[Double](11))
    pstmt.setDouble(14,value.getAs[Double](12))
    // 操作失误  14 写成了 10 导致报错    索引中丢失 IN 或 OUT 参数:: 14
    pstmt.setInt(15,value.getAs[Integer](13))

//    if(group_type != 0){
      //println(value.getAs[Integer](13))
      if("".equals(value.getAs[Integer](14)) || value.getAs[Integer](14) != null){
        pstmt.setInt(16,value.getAs[Integer](14))
      }else{
        pstmt.setInt(16,-1)
      }


//    }else{
//      pstmt.setInt(16,0)
//    }

    pstmt.setInt(17,value.getAs[Integer](16))


    pstmt.setString(18,value.getAs[String](15))
    pstmt.setInt(19,value.getAs[Integer](0))
    pstmt.setInt(20,value.getAs[Integer](1))
    pstmt.setInt(21,value.getAs[Integer](2))
    pstmt.setInt(22,value.getAs[Integer](4))
    pstmt.setInt(23,value.getAs[Integer](3))
    pstmt.setInt(24,value.getAs[Integer](5))


    pstmt.setLong(25,value.getAs[Long](6))
    if(value.getAs[Double](7) == 99999){

      pstmt.setNull(26,Types.DOUBLE)
      pstmt.setNull(29,Types.DOUBLE)
    }else{
      pstmt.setDouble(26,value.getAs[Double](7))
      pstmt.setDouble(29,value.getAs[Double](10))
    }
//    pstmt.setDouble(26,value.getAs[Double](7))
    pstmt.setDouble(27,value.getAs[Double](8))
    pstmt.setDouble(28,value.getAs[Double](9))
//    pstmt.setDouble(29,value.getAs[Double](10))
    pstmt.setDouble(30,value.getAs[Double](11))
    pstmt.setDouble(31,value.getAs[Double](12))
    // 操作失误  14 写成了 10 导致报错    索引中丢失 IN 或 OUT 参数:: 14
    pstmt.setInt(32,value.getAs[Integer](13))

//    if(group_type != 0){
      //println(value.getAs[Integer](13))
      if("".equals(value.getAs[Integer](14)) || value.getAs[Integer](14) != null){
        pstmt.setInt(33,value.getAs[Integer](14))
      }else{
        pstmt.setInt(33,-1)
      }


//    }else{
//      pstmt.setInt(33,0)
//    }

    pstmt.setInt(34,value.getAs[Integer](16))



    pstmt.addBatch()



//    pstmt.setString(1,value.getAs[String](15))
//    pstmt.setInt(2,value.getAs[Integer](0))
//    pstmt.setInt(3,value.getAs[Integer](1))
//    pstmt.setInt(4,value.getAs[Integer](2))
//    pstmt.setInt(5,value.getAs[Integer](4))
//    pstmt.setInt(6,value.getAs[Integer](3))
//    pstmt.setInt(7,value.getAs[Integer](5))
//
//
//    pstmt.setLong(8,value.getAs[Long](6))
//    pstmt.setDouble(9,value.getAs[Double](7))
//    pstmt.setDouble(10,value.getAs[Double](8))
//    pstmt.setDouble(11,value.getAs[Double](9))
//    pstmt.setDouble(12,value.getAs[Double](10))
//    pstmt.setDouble(13,value.getAs[Double](11))
//    pstmt.setDouble(14,value.getAs[Double](12))
//
//    // 操作失误  14 写成了 10 导致报错    索引中丢失 IN 或 OUT 参数:: 14
//    pstmt.setInt(15,value.getAs[Integer](13))
//
//    if(group_type != 0){
////      println(value.getAs[Integer](13))
//      if("".equals(value.getAs[Integer](14)) || value.getAs[Integer](14) != null){
//        pstmt.setInt(16,value.getAs[Integer](14))
//      }else{
//        pstmt.setInt(16,-1)
//      }
//
//
//    }else{
//      pstmt.setInt(16,0)
//    }
//
//    pstmt.setInt(17,value.getAs[Integer](16))
//    pstmt.addBatch()
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


//
//  def main(args: Array[String]) {
//
//
//    println(getConnect())
//    val aa = 11f
//    11.getClass.getName
//
//    println(aa.isInstanceOf[Int])
//  }

}
