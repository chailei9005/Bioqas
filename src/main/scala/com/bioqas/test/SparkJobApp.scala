package com.bioqas.test

import java.util.concurrent.{Callable, ExecutorService, Executors}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by chailei on 18/11/14.
  */
object SparkJobApp {


  def main(args: Array[String]) {



    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("SparkJobApp")

    val sc = new SparkContext(sparkConf)



    val lines: RDD[String] = sc.textFile("/Users/chailei/data/wc.txt")

    val lines1: RDD[String] = sc.textFile("/Users/chailei/data/wc1.txt")


    val lines2: RDD[String] = sc.textFile("/Users/chailei/data/wc.txt")

    val lines3: RDD[String] = sc.textFile("/Users/chailei/data/wc1.txt")



    println(lines.count())

    println(lines1.count())


    val threadPool: ExecutorService = Executors.newFixedThreadPool(2)

    threadPool.submit(new Runnable {
      override def run(): Unit = {
        println(lines2.count())

      }
    })


    threadPool.submit(new Runnable {
      override def run(): Unit = {
        println(lines3.count())

      }
    })


    threadPool.shutdown()








    Thread.sleep(1000000)


    sc.stop()

  }

}
