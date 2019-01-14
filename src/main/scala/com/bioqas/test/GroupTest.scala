package com.bioqas.test

import com.bioqas.utils.IdAccumulator
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * 测试group 触发
  */
object GroupTest {


  def main(args: Array[String]) {


    val conf = new SparkConf()
      .setAppName("TestApp")
      .setMaster("local[4]")
      .set("spark.streaming.kafka.maxRatePerPartition","1")
    //      .set("spark.executor.extraJavaOptions", "-XX:+UseConcMarkSweepGC")
    //      .set("spark.streaming.kafka.maxRatePerPartition", "1000")
    //      .set("spark.default.parallelism", "20")
    //      "metadata.broker.list" -> "192.168.8.155:9092,192.168.8.156:9092,192.168.8.157:9092"

    // metadata.broker.list 参数为什么不能用ip:端口
    val paramMap = Map[String, Object](
      "bootstrap.servers" -> "biaoyuan01:9092,biaoyuan02:9092,biaoyuan03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "max.partition.fetch.bytes" -> (2621440: java.lang.Integer), //default: 1048576
      "request.timeout.ms" -> (90000: java.lang.Integer), //default: 60000
      "session.timeout.ms" -> (60000: java.lang.Integer) //default: 30000
    )
    //"metadata.broker.list" -> "mini2:9092,mini3:9092,mini4:9092"
    val topics = "group"
    val topicSet = topics.split(",")
    val ssc = new StreamingContext(conf, Seconds(5))

    val kafkaStream = KafkaUtils
      .createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicSet, paramMap)
    )

    val accu = ssc.sparkContext.longAccumulator("myflag")
    val rddaccu = ssc.sparkContext.longAccumulator("count")
    val myAcc = new IdAccumulator
    val idAcc = new IdAccumulator
    ssc.sparkContext.register(myAcc,"myAcc")
    ssc.sparkContext.register(idAcc,"idAcc")
    val ids = mutable.HashSet[String]()
    val idBroadCast = ssc.sparkContext.broadcast(ids)
    JdbcDialects.registerDialect(OracleDialect)
    kafkaStream.foreachRDD(rdd => {
      rdd.foreach(println)
    })


    ssc.start()
    ssc.awaitTermination()

    ssc.stop()


  }

}
