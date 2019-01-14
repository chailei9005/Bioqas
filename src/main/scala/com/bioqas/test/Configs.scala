package com.bioqas.test

import com.bioqas.utils.Constants
import com.typesafe.config.{Config, ConfigFactory}


/**
  * Created by chailei on 18/12/19.
  */
object Configs {


  val load: Config = ConfigFactory.load()


  def getString(key: String) = {
    load.getString(key)
  }

  def getInt(key: String) = {
    load.getInt(key)
  }


  def main(args: Array[String]) {




    println(getString(Constants.ZOOKEEPER_SERVER))


    println(getString(Constants.KAFKA_SERVER))


    println(getString(Constants.DB_URL))


    println(getString(Constants.DB_DRIVER))


    println(getString(Constants.DB_USER))


    println(getString(Constants.DB_PASSWORD))


    println(getString(Constants.DB_DATABASE))

    println(getInt(Constants.STREAMING_REPORT_DATA))

    println(getInt(Constants.STREAMING_GROUP_DATA))


    println(getString(Constants.PHONE_URL))





  }

}
