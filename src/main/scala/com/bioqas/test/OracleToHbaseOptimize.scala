package com.bioqas.test

import java.util.Date

import com.bioqas.utils.{IdAccumulator, PhoenixUtils, JSONParseUtil}
import org.apache.commons.lang3.time.FastDateFormat

import org.apache.spark.SparkConf

import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.phoenix.spark._  // phoenixTableAsDataFrame
import scala.collection.mutable
/**
  * Created by chailei on 18/7/5.
  */
object OracleToHbaseOptimize {


    def main(args: Array[String]) {

      val timeFormat = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss.SSS")
      println("启动时间：" + timeFormat.format(new Date()))

      val conf = new SparkConf()
//        .setAppName("TestApp")
//        .setMaster("local[4]")
//        .set("spark.executor.extraJavaOptions", "-XX:+UseConcMarkSweepGC")
//        .set("spark.streaming.kafka.maxRatePerPartition", "1000")
//        .set("spark.default.parallelism", "400")
      //      "metadata.broker.list" -> "192.168.8.155:9092,192.168.8.156:9092,192.168.8.157:9092"

      // metadata.broker.list 参数为什么不能用ip:端口
      val paramMap = Map[String, Object](
        "bootstrap.servers" -> "biaoyuan01:9092,biaoyuan02:9092,biaoyuan03:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "test",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean),
        "max.partition.fetch.bytes" -> (2621440: java.lang.Integer), //default: 1048576
        "request.timeout.ms" -> (90000: java.lang.Integer), //default: 60000
        "session.timeout.ms" -> (60000: java.lang.Integer) //default: 30000
      )
      //"metadata.broker.list" -> "mini2:9092,mini3:9092,mini4:9092"
//      val topics = "bioqas"
      val topicSet = Array("bioqas")
      val ssc = new StreamingContext(conf, Seconds(20))

      val kafkaStream = KafkaUtils
        .createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topicSet, paramMap)
      )

      val accu = ssc.sparkContext.longAccumulator("myflag")
      val myAcc = new IdAccumulator
      ssc.sparkContext.register(myAcc,"myAcc")
      val ids = mutable.HashSet[String]()
      val idBroadCast = ssc.sparkContext.broadcast(ids)

      kafkaStream.foreachRDD(rdd => {

        if (rdd != null) {

          val offsetRangs = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

          println("------------rdd begin process-------------------------")
          //        println("===========accu = " + accu.value)
          idBroadCast.value.clear()

          rdd.foreachPartition(partitions => {

            println("*********partitions qian**********")
            if (partitions != null) {


              println("-------create phoenixutil time"+timeFormat.format(new Date())+"--------------------")
              val phoenixUtil = new PhoenixUtils()
              println("-------end phoenixutil time"+timeFormat.format(new Date())+"--------------------")

              var batchSize = 0
              val commitSize = 3000
//              var flag = 0

              partitions.foreach(line => {

                println("********partition hou***********")
                val value = line.value()

                //          println(value)
                println("-------start parser sql time"+timeFormat.format(new Date())+"--------------------")
                val sql = JSONParseUtil.parseJSONToPhoenixSQL(value.trim)
                //              myAcc.add(sql._2)
                println("-------end parser sql time"+timeFormat.format(new Date())+"--------------------")

                println("-------start save to hbase time"+timeFormat.format(new Date())+"--------------------")

                phoenixUtil.saveToHbase(sql._1)

                batchSize = batchSize + 1

                if (batchSize % commitSize == 0) {
                  phoenixUtil.conn.commit()
                }
                println("-------end save to hbase time"+timeFormat.format(new Date())+"--------------------")


              })
              println("------------last commit and close connection-------------------------")
              phoenixUtil.closeConn()



            }
          })

          kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRangs)

        }


      })

      println("================================")



      ssc.start()
      ssc.awaitTermination()


    }


}
