package com.bioqas.product

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.spark.sql.Row

/**
  * Created by chailei on 18/7/24.
  */
object OracleGroupPPDbTestUtils {

  val driver = "oracle.jdbc.driver.OracleDriver"
//  val url = "jdbc:oracle:thin:@192.168.8.108:1521:orcl"
  val url = "jdbc:oracle:thin:@47.92.136.8:1521:biodev"
//  val username = "test"
  val username = "biodev"
//  val password = "test"
  val password = "Bio#2018#Ora"
  val database = "biodev"


//  val sql = s"MERGE INTO ${database}.TP_E_BD_GROUP_STATISTICS_PP t1 USING (SELECT ? ID ,? CENTER_ID, ? PLAN_ID, ? SPEC_ID," +
//    "? TIMES_ID,? ITEM_ID,? GROUP_ID,? PROGRESS FROM dual) t2 ON " +
//    "(t1.ID = t2.ID) WHEN MATCHED THEN UPDATE SET t1.PROGRESS = t2.PROGRESS WHEN NOT " +
//    "MATCHED THEN INSERT (ID,CENTER_ID,PLAN_ID,SPEC_ID,TIMES_ID,ITEM_ID,GROUP_ID,PROGRESS) VALUES(?,?,?,?,?,?,?,?)"


  val sql = s"insert into ${database}.TP_E_BD_GROUP_STATISTICS_PP(ID,Center_Id,Plan_Id,Spec_Id,Times_Id,Item_Id,Group_Id," +
    s"PROGRESS) select ?,?,?,?,?,?,?,? FROM dual " +
    s"where not exists(select * from TP_E_BD_GROUP_STATISTICS_PP where " +
    s"id=? and Center_Id=? and Plan_Id=? and Spec_Id=? and Times_Id=? and Item_Id=? and Group_Id=? and PROGRESS=?)"

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


    // "CENTER_ID","PLAN_ID","SPEC_ID","TIMES_ID","ITEM_ID","GROUP_ID"
    pstmt.setString(1,pk_id)
    pstmt.setInt(2,value.getAs[Integer](0))
    pstmt.setInt(3,value.getAs[Integer](1))
    pstmt.setInt(4,value.getAs[Integer](2))
    pstmt.setInt(5,value.getAs[Integer](3))
    pstmt.setInt(6,value.getAs[Integer](4))
    pstmt.setInt(7,value.getAs[Integer](5))
    pstmt.setInt(8,value.getAs[Integer](6))

    pstmt.setString(9,pk_id)
    pstmt.setInt(10,value.getAs[Integer](0))
    pstmt.setInt(11,value.getAs[Integer](1))
    pstmt.setInt(12,value.getAs[Integer](2))
    pstmt.setInt(13,value.getAs[Integer](3))
    pstmt.setInt(14,value.getAs[Integer](4))
    pstmt.setInt(15,value.getAs[Integer](5))
    pstmt.setInt(16,value.getAs[Integer](6))


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
