package com.bioqas.product

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.spark.sql.Row

/**
  * Created by chailei on 18/7/24.
  */
object OracleLabDbTestUtils {

  val driver = "oracle.jdbc.driver.OracleDriver"
//  val url = "jdbc:oracle:thin:@192.168.8.108:1521:orcl"
  val url = "jdbc:oracle:thin:@47.92.136.8:1521:biodev"
//  val username = "test"
  val username = "biodev"
//  val password = "test"
  val password = "Bio#2018#Ora"
  val database = "biodev"

  val sql = s"INSERT into ${database}.TP_E_BD_Target_pec_lab_insert(ID,CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,GROUP_ID," +
    "GROUP_NAME,BATCH_NUM,TARGET,LAB_ID,LAB_NAME,LAB_SPELL,SCORE) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

//
//  val sql = s"MERGE INTO ${database}.TP_E_BD_Target_pec_lab_insert t1 USING (SELECT ? ID ,? CENTER_ID, ? PLAN_ID, ? SPEC_ID,? ITEM_ID," +
//    "? TIMES_ID,? BATCH_ID,? GROUP_ID,? GROUP_NAME,? BATCH_NUM,? TARGET," +
//    "? LAB_ID,? LAB_NAME,? LAB_SPELL,? SCORE FROM dual) t2 ON " +
//    "(t1.ID = t2.ID) WHEN MATCHED THEN UPDATE SET t1.TARGET = t2.TARGET,t1.score = t2.score WHEN NOT " +
//    "MATCHED THEN INSERT (ID,CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,GROUP_ID," +
//    "GROUP_NAME,BATCH_NUM,TARGET,LAB_ID,LAB_NAME,LAB_SPELL,SCORE) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

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


//    |-- CENTER_ID: string (nullable = true) 0
//    |-- PLAN_ID: string (nullable = true) 1
//    |-- SPEC_ID: string (nullable = true)2
//    |-- ITEM_ID: string (nullable = true)3
//    |-- TIMES_ID: string (nullable = true)4
//    |-- BATCH_ID: string (nullable = true)5
//    |-- LAB_ID: string (nullable = true)6
//    |-- REPORT_DATA: string (nullable = true)7
//    |-- LAB_NAME: string (nullable = true)8
//    |-- LAB_SPELL: string (nullable = true)9
//    |-- GROUP_ID: string (nullable = true)10
//    |-- GROUP_NAME: string (nullable = true)11
//    |-- target: string (nullable = true)12
//    |-- score: integer (nullable = true)13
//    |-- BATCH_NAME: string (nullable = true)14
//    println(value.toString())
    //    ID,CENTER_ID,PLAN_ID,SPEC_ID,TIMES_ID,ITEM_ID,BATCH_ID,GROUP_ID," +
    //    "GROUP_NAME,BATCH_NUM,TARGET,LAB_ID,LAB_NAME,LAB_SPELL,SCORE
    pstmt.setString(1,pk_id)
    pstmt.setInt(2,value.getAs[Integer](0).toInt)
    pstmt.setInt(3,value.getAs[Integer](1).toInt)
    pstmt.setInt(4,value.getAs[Integer](2).toInt)
    pstmt.setInt(5,value.getAs[Integer](4).toInt)
    pstmt.setInt(6,value.getAs[Integer](3).toInt)
    pstmt.setInt(7,value.getAs[Integer](5).toInt)


    pstmt.setInt(8,value.getAs[Integer](11))
    pstmt.setString(9,value.getAs[String](12))
    pstmt.setString(10,value.getAs[String](10))

    pstmt.setDouble(11,value.getAs[String](13).toDouble)

    pstmt.setInt(12,value.getAs[Integer](6))
    pstmt.setString(13,value.getAs[String](8))
    pstmt.setString(14,value.getAs[String](9))
    pstmt.setInt(15,value.getAs[Integer](14))

//    pstmt.setString(16,pk_id)
//    pstmt.setInt(17,value.getAs[Integer](0).toInt)
//    pstmt.setInt(18,value.getAs[Integer](1).toInt)
//    pstmt.setInt(19,value.getAs[Integer](2).toInt)
//    pstmt.setInt(20,value.getAs[Integer](4).toInt)
//    pstmt.setInt(21,value.getAs[Integer](3).toInt)
//    pstmt.setInt(22,value.getAs[Integer](5).toInt)
//
//
//    pstmt.setInt(23,value.getAs[Integer](11))
//    pstmt.setString(24,value.getAs[String](12))
//    pstmt.setString(25,value.getAs[String](10))
//
//    pstmt.setDouble(26,value.getAs[String](13).toDouble)
//
//    pstmt.setInt(27,value.getAs[Integer](6))
//    pstmt.setString(28,value.getAs[String](8))
//    pstmt.setString(29,value.getAs[String](9))
//    pstmt.setInt(30,value.getAs[Integer](14))


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
