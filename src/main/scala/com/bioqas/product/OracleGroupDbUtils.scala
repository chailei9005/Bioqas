package com.bioqas.product

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import com.bioqas.test.Configs
import com.bioqas.utils.Constants
import org.apache.spark.sql.Row

/**
  * Created by chailei on 18/7/24.
  */
object OracleGroupDbUtils {

  val driver = "oracle.jdbc.driver.OracleDriver"
//  val url = "jdbc:oracle:thin:@192.168.8.108:1521:orcl"
  val url = "jdbc:oracle:thin:@47.92.136.8:1521:biodev"
//  val username = "test"
  val username = "biodev"
//  val password = "test"
  val password = "Bio#2018#Ora"
  val database = "ZPGLXT"

//  val insertSql = s"insert into ${database}.TP_E_BD_Target_percentage(" +
//    "ID,CENTER_ID,PLAN_ID,SPEC_ID,TIMES_ID," +
//    "ITEM_ID,BATCH_ID,GROUP_ID,TARGET," +
//    "QUALIFIED_PERCENT,UNQUALIFIED_PERCENT) values(" +
//    "?,?,?,?,?," +
//    "?,?,?,?,?," +
//    "?)"
//
//  val updateSql = s"update ${database}.TP_E_BD_Target_percentage set TARGET=?," +
//    "QUALIFIED_PERCENT=?,UNQUALIFIED_PERCENT=? where ID=?"
//
//  val querySql = s"select * from ${database}.TP_E_BD_Target_percentage where ID = ?"

  val sql = s"MERGE INTO ${Configs.getString(Constants.DB_DATABASE)}.TP_E_BD_Target_percentage t1 USING (SELECT ? ID ,? CENTER_ID, ? PLAN_ID, ? SPEC_ID,? TIMES_ID," +
    "? ITEM_ID,? BATCH_ID,? GROUP_ID,? GROUP_NAME,? BATCH_NUM,? TARGET," +
    "? QUALIFIED_PERCENT,? UNQUALIFIED_PERCENT FROM dual) t2 ON " +
    "(t1.ID = t2.ID) WHEN MATCHED THEN UPDATE SET t1.TARGET = t2.TARGET,t1.QUALIFIED_PERCENT = t2.QUALIFIED_PERCENT " +
    ",t1.UNQUALIFIED_PERCENT = t2.UNQUALIFIED_PERCENT WHEN NOT " +
    "MATCHED THEN INSERT (ID,CENTER_ID,PLAN_ID,SPEC_ID,TIMES_ID,ITEM_ID,BATCH_ID,GROUP_ID," +
    "GROUP_NAME,BATCH_NUM,TARGET,QUALIFIED_PERCENT,UNQUALIFIED_PERCENT) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"

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
//    "GROUP_NAME,BATCH_NUM,TARGET,QUALIFIED_PERCENT,UNQUALIFIED_PERCENT
//    |-- CENTER_ID: string (nullable = true)
//    |-- PLAN_ID: string (nullable = true)
//    |-- SPEC_ID: string (nullable = true)
//    |-- ITEM_ID: string (nullable = true)
//    |-- TIMES_ID: string (nullable = true)
//    |-- BATCH_ID: string (nullable = true)
//    |-- GROUP_ID: string (nullable = true)
//    |-- GROUP_NAME: string (nullable = true)
//    |-- BATCH_NAME: string (nullable = true)
//    |-- target: string (nullable = true)
//    |-- per1: double (nullable = true)
//    |-- per2: double (nullable = true)

    pstmt.setString(1,pk_id)
    pstmt.setInt(2,value.getAs[Integer](0).toInt)
    pstmt.setInt(3,value.getAs[Integer](1).toInt)
    pstmt.setInt(4,value.getAs[Integer](2).toInt)
    pstmt.setInt(5,value.getAs[Integer](3).toInt)
    pstmt.setInt(6,value.getAs[Integer](4).toInt)
    pstmt.setInt(7,value.getAs[Integer](5).toInt)


    pstmt.setInt(8,value.getAs[Integer](6).toInt)
    pstmt.setString(9,value.getAs[String](7))
    pstmt.setString(10,value.getAs[String](8))
    pstmt.setDouble(11,value.getAs[String](9).toDouble)
    pstmt.setDouble(12,value.getAs[Double](10))
    pstmt.setDouble(13,value.getAs[Double](11))

    pstmt.setString(14,pk_id)
    pstmt.setInt(15,value.getAs[Integer](0).toInt)
    pstmt.setInt(16,value.getAs[Integer](1).toInt)
    pstmt.setInt(17,value.getAs[Integer](2).toInt)
    pstmt.setInt(18,value.getAs[Integer](3).toInt)
    pstmt.setInt(19,value.getAs[Integer](4).toInt)
    pstmt.setInt(20,value.getAs[Integer](5).toInt)


    pstmt.setInt(21,value.getAs[Integer](6).toInt)
    pstmt.setString(22,value.getAs[String](7))
    pstmt.setString(23,value.getAs[String](8))
    pstmt.setDouble(24,value.getAs[String](9).toDouble)
    pstmt.setDouble(25,value.getAs[Double](10))
    pstmt.setDouble(26,value.getAs[Double](11))

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
