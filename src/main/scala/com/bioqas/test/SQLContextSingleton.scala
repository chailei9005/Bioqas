package com.bioqas.test

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by chailei on 18/6/12.
  */
object SQLContextSingleton {
  @transient  private var instance: SQLContext = _
  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}
