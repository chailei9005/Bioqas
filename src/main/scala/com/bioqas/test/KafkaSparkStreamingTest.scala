package com.bioqas.test


import java.util.Date

//import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/**
  * Created by chailei on 18/6/19.
  */
object KafkaSparkStreamingTest {


  def main(args: Array[String]) {


    val conf = new SparkConf()
      .setAppName("TestApp")
      .setMaster("local[4]")
      .set("spark.executor.extraJavaOptions","-XX:+UseConcMarkSweepGC")
      .set("spark.streaming.kafka.maxRatePerPartition","1000")
    //      "metadata.broker.list" -> "192.168.8.155:9092,192.168.8.156:9092,192.168.8.157:9092"

    // metadata.broker.list 参数为什么不能用ip:端口
    val paramMap = Map[String,String](
      "metadata.broker.list" -> "biaoyuan01:9092,biaoyuan02:9092,biaoyuan03:9092"
    )
    //"metadata.broker.list" -> "mini2:9092,mini3:9092,mini4:9092"
    val topics = "bioqas"
    val topicSet = topics.split(",")
    val ssc = new StreamingContext(conf,Seconds(20))

    val kafkaStream = KafkaUtils
      .createDirectStream[String,String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicSet, paramMap)
    )
    println("==============Kafka Streaming=================")

    var offsetRanges = Array.empty[OffsetRange]

    kafkaStream.transform { rdd =>
      println("==========start time "+new Date()+"=============")
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.foreachRDD { rdd =>

      for (o <- offsetRanges) {
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
//      rdd.collect().take(200)
      println("==========end time "+new Date()+"=============")

    }



//
//    kafkaStream.foreachRDD(rdd => {
//
//
//
//
//    })


    ssc.start()
    ssc.awaitTermination()

  }

}
