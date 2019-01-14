package com.bioqas.product

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import com.bioqas.test.Configs
import com.bioqas.utils.Constants
import org.apache.spark.sql.Row

/**
  * Created by chailei on 18/7/24.
  */
object OracleWjDbUtils {

  val driver = "oracle.jdbc.driver.OracleDriver"
//  val url = "jdbc:oracle:thin:@192.168.8.108:1521:orcl"
  val url = "jdbc:oracle:thin:@47.92.136.8:1521:biodev"
//  val username = "test"
  val username = "biodev"
//  val password = "test"
  val password = "Bio#2018#Ora"
  val database = "ZPGLXT"


  val sql = s"MERGE INTO ${Configs.getString(Constants.DB_DATABASE)}.TP_E_BD_GROUP_STATISTICS t1 USING (SELECT ? ID ,? CENTER_ID, ? PLAN_ID, ? SPEC_ID,? TIMES_ID," +
    "? ITEM_ID,? BATCH_ID,? GROUP_ID,? GROUP_NAME,? BATCH_NUM,? STATISTICS_COUNT," +
    "? STATISTICS_AVG,? STATISTICS_MEDIAN,? STATISTICS_SD,? STATISTICS_CV,? STATISTICS_MAX,? STATISTICS_MIN,? Robust_mean," +
    "? Robust_SD,? Robust_med,? Robust_niqr,? DISCARD_NSD FROM dual) t2 ON " +
    "(t1.ID = t2.ID) WHEN MATCHED THEN UPDATE SET t1.STATISTICS_COUNT = t2.STATISTICS_COUNT,t1.STATISTICS_AVG = t2.STATISTICS_AVG " +
    ",t1.STATISTICS_MEDIAN = t2.STATISTICS_MEDIAN,t1.STATISTICS_SD = t2.STATISTICS_SD,t1.STATISTICS_CV = t2.STATISTICS_CV," +
    "t1.STATISTICS_MAX = t2.STATISTICS_MAX,t1.STATISTICS_MIN = t2.STATISTICS_MIN,t1.Robust_mean = t2.Robust_mean," +
    "t1.Robust_SD = t2.Robust_SD,t1.Robust_med = t2.Robust_med,t1.Robust_niqr = t2.Robust_niqr,t1.DISCARD_NSD = t2.DISCARD_NSD WHEN NOT " +
    "MATCHED THEN INSERT (ID,CENTER_ID,PLAN_ID,SPEC_ID,TIMES_ID,ITEM_ID,BATCH_ID,GROUP_ID," +
    "GROUP_NAME,BATCH_NUM,STATISTICS_COUNT,STATISTICS_AVG,STATISTICS_MEDIAN,STATISTICS_SD,STATISTICS_CV,STATISTICS_MAX," +
    "STATISTICS_MIN,Robust_mean,Robust_SD,Robust_med,Robust_niqr,DISCARD_NSD) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

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

  def insertBatch(pstmt: PreparedStatement,value:Row,pk_id: String): Unit ={

//    ID,CENTER_ID,PLAN_ID,SPEC_ID,TIMES_ID,ITEM_ID,BATCH_ID,GROUP_ID," +
//    "GROUP_NAME,BATCH_NUM,STATISTICS_COUNT,STATISTICS_AVG,STATISTICS_MEDIAN,STATISTICS_SD,STATISTICS_CV,STATISTICS_MAX," +
//      "STATISTICS_MIN,Robust_mean,Robust_SD,Robust_med,Robust_niqr,DISCARD_NSD

    pstmt.setString(1,pk_id)
    pstmt.setInt(2,value.getAs[Integer](0).toInt)
    pstmt.setInt(3,value.getAs[Integer](1).toInt)
    pstmt.setInt(4,value.getAs[Integer](2).toInt)
    pstmt.setInt(5,value.getAs[Integer](3).toInt)
    pstmt.setInt(6,value.getAs[Integer](4).toInt)

    pstmt.setInt(7,value.getAs[Integer](5).toInt)

    pstmt.setString(9,value.getAs[String](8))
    pstmt.setInt(8,value.getAs[Integer](7))
    pstmt.setString(10,value.getAs[String](6))

    pstmt.setLong(11,value.getAs[Long](9))
    pstmt.setDouble(12,value.getAs[Double](10))
    pstmt.setDouble(13,value.getAs[Double](11))
    pstmt.setDouble(14,value.getAs[Double](12))
    pstmt.setDouble(15,value.getAs[Double](13))
    pstmt.setDouble(16,value.getAs[Double](14))
    pstmt.setDouble(17,value.getAs[Double](15))
    pstmt.setDouble(18,value.getAs[Double](16))
    pstmt.setDouble(19,value.getAs[Double](17))
    pstmt.setDouble(20,value.getAs[Double](18))
    pstmt.setDouble(21,value.getAs[Double](19))

    pstmt.setInt(22,value.getAs[String](20).toInt)

    pstmt.setString(23,pk_id)
    pstmt.setInt(24,value.getAs[Integer](0).toInt)
    pstmt.setInt(25,value.getAs[Integer](1).toInt)
    pstmt.setInt(26,value.getAs[Integer](2).toInt)
    pstmt.setInt(27,value.getAs[Integer](3).toInt)
    pstmt.setInt(28,value.getAs[Integer](4).toInt)
    pstmt.setInt(29,value.getAs[Integer](5).toInt)


    pstmt.setString(31,value.getAs[String](8))
    pstmt.setInt(30,value.getAs[Integer](7))
    pstmt.setString(32,value.getAs[String](6))

    pstmt.setLong(33,value.getAs[Long](9))
    pstmt.setDouble(34,value.getAs[Double](10))
    pstmt.setDouble(35,value.getAs[Double](11))
    pstmt.setDouble(36,value.getAs[Double](12))
    pstmt.setDouble(37,value.getAs[Double](13))
    pstmt.setDouble(38,value.getAs[Double](14))
    pstmt.setDouble(39,value.getAs[Double](15))
    pstmt.setDouble(40,value.getAs[Double](16))
    pstmt.setDouble(41,value.getAs[Double](17))
    pstmt.setDouble(42,value.getAs[Double](18))
    pstmt.setDouble(43,value.getAs[Double](19))

    pstmt.setInt(44,value.getAs[String](20).toInt)


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
