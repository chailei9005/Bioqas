package com.bioqas.test

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Date

/**
  * Created by chailei on 18/10/31.
  */
object OracleBatchValuesTest {

  val driver = "oracle.jdbc.driver.OracleDriver"
  val url = "jdbc:oracle:thin:@47.92.136.8:1521:biodev"
  val username = "biodev"
  val password = "Bio#2018#Ora"
  val database = "biodev"


//  val sql1 = "insert into BATCHCOMMIT from where"

//  val sql1 = "select "


  val sql = s"select * from ${database}.BATCHCOMMIT where ? in (select * from ${database}.BATCHCOMMIT)"


  def main(args: Array[String]) {


    val connect: Connection = getConnect()
    connect.setAutoCommit(false)

    val prepareStatement: PreparedStatement = connect.prepareStatement(sql)

    for(value <- 1 to 6000){

      insertBatch(prepareStatement,"aaa"+value,"chailei" + value)
    }

    println("=====insert before=====")

    val startTime: Long = new Date().getTime
    val batch: Array[Int] = prepareStatement.executeBatch()
    val endTime: Long = new Date().getTime

    println("time = " + (endTime-startTime)/1000)
    println("=====insert after=====")




    println(batch.toSet.contains(-2))




    batch.foreach(print)

    connect.commit()




  }

  def insertBatch(pstmt: PreparedStatement,id: String, name: String): Unit ={
    pstmt.setString(1,id)
    pstmt.setString(2,name)
    pstmt.setString(3,id)
    pstmt.setString(4,name)
    pstmt.addBatch()

  }


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

}
