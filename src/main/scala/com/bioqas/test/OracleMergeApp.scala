package com.bioqas.test

import java.sql.{PreparedStatement, DriverManager, Connection}

/**
  * Created by chailei on 18/9/21.
  */
object OracleMergeApp {

  val driver = "oracle.jdbc.driver.OracleDriver"
  val url = "jdbc:oracle:thin:@192.168.8.118:1521:orcl"
  val username = "bioqas"
  val password = "bioqas"

  val sql0 = "MERGE INTO TESTKEY t1 USING (SELECT ? id ,? name FROM dual) t2 ON (t1.ID = t2.id) WHEN MATCHED THEN UPDATE SET t1.NAME = t2.name,t1.NAME = t2.name WHEN NOT MATCHED THEN INSERT (ID,NAME) VALUES(?,?)"
  val sql1 = "MERGE INTO TESTKEY t1 USING (SELECT '?' id ,'?' name FROM dual) t2 ON (t1.ID = t2.id) WHEN MATCHED THEN UPDATE SET t1.NAME = t2.name WHEN NOT MATCHED THEN INSERT (ID,NAME) VALUES('105','chailei22')"


  def main(args: Array[String]) {

    val connect: Connection = getConnect()
    connect.setAutoCommit(false)
    val pstmt : PreparedStatement  = connect.prepareStatement(sql0)
    for(i <- 100 until 120){
      pstmt.setString(1,i.toString)
      pstmt.setString(2,"chailei"+i)
      pstmt.setString(3,i.toString)
      pstmt.setString(4,"origin")
      pstmt.addBatch()
    }
    pstmt.executeBatch()
    connect.commit()
//
    pstmt.execute()
    connect.close()


  }

  def getConnect() : Connection = {

    Class.forName(driver)

    val connection: Connection = DriverManager.getConnection(url, username, password)

    connection
  }
}
