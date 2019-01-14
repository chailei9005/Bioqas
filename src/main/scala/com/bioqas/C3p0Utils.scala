package com.bioqas

import java.sql.{PreparedStatement, Connection}

import com.mchange.v2.c3p0.ComboPooledDataSource

/**
  * Created by chailei on 18/11/27.
  */
object C3p0Utils {


   val comboPooledDataSource = new ComboPooledDataSource()


  def getConnect() = {

    comboPooledDataSource.getConnection

  }

  def release(connection: Connection, pstmt: PreparedStatement) = {

    pstmt.close()
    connection.close()
  }


}
