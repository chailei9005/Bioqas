package com.bioqas.test


import org.apache.spark.SparkConf

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.{ Seconds, StreamingContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/**
  * Created by chailei on 18/6/6.
  */

case class AfterRecord(ID:Int,CENTER_ID:Int,PLAN_ID:Int,PLAN_NAME:String,PLAN_CREATE_USER_ID:Int,SPEC_ID:Int
                  ,SPEC_NAME:String,TIMES_ID:Int,TIMES_NAME:String,ITEM_ID:Int,ITEM_NAME:String,ITEM_TYPE:Int
                  ,ITEM_UNITS_NAME:String,BATCH_ID:Int,BATCH_NAME:String,REPORT_DATA:String,METHOD_ID:Int,METHOD_NAME:String
                  ,LAB_INS_ID:Int,LAB_INS_NAME:String,INS_ID:Int,INS_NAME:String,INS_BRANDS_ID:Int,INS_BRANDS_NAME:String
                  ,REAGENT_NAME_ID:Int,REAGENT_NAME:String,REAGENT_BRANDS_ID:Int,REAGENT_BRANDS_NAME:String,CAL_MATERIAL_BRANDS_ID:Int,CAL_MATERIAL_BRANDS_NAME:String
                  ,INPUT_TYPE:Int,INPUT_DATE:String,INPUT_USER_ID:Int,REPORT_TYPE:Int,REPORT_DATE:String,REPORT_USER_ID:Int
                  ,LAB_ID:Int,LAB_NAME:String,LAB_CODE:String,LAB_SPELL:String,LAB_LONGITUDE:String,LAB_LATITUDE:String
                  ,CREATE_DATE:String,IS_DELETE:Int,DELETE_DATE:String)

case class Record(table:String,op_type:String,op_ts:String,current_ts:String,pos:String,after:AfterRecord)

object TestApp {

  def main(args: Array[String]) {


    val conf = new SparkConf()
      .setAppName("TestApp")
      .setMaster("local[4]")
      .set("spark.executor.extraJavaOptions","-XX:+UseConcMarkSweepGC")
      .set("spark.streaming.kafka.maxRatePerPartition","1000")
      .set("spark.default.parallelism","20")
//      "metadata.broker.list" -> "192.168.8.155:9092,192.168.8.156:9092,192.168.8.157:9092"

    // metadata.broker.list 参数为什么不能用ip:端口
    val paramMap = Map[String,String](
      "metadata.broker.list" -> "biaoyuan01:9092,biaoyuan02:9092,biaoyuan03:9092"
    )
    //"metadata.broker.list" -> "mini2:9092,mini3:9092,mini4:9092"
    val topics = "bioqas"
    val topicSet = topics.split(",")
    val ssc = new StreamingContext(conf,Seconds(10))

    val kafkaStream = KafkaUtils
      .createDirectStream[String,String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicSet, paramMap)
    )

    println("==============Kafka Streaming=================")

//      kafkaStream.map((_._2)).print()
//    val message = kafkaStream.map(_._2)
//                  .map(value => {
//                    implicit val formats: DefaultFormats.type = DefaultFormats
//                      val json = parse(value)
//                    json.extract[Record]
//                  })
//      .foreachRDD( rdd => {
//      rdd.foreachPartition( partitionRecord =>{
//
//        partitionRecord.foreach(println)
//      }
//
//      )
//    })
    // 为什么每个批次都执行 因为没有判断rdd是否为空
//    kafkaStream.foreachRDD( rdd => {
//
////      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
//      if(!rdd.isEmpty()){
//
//      val ss = SparkSession.builder().config(conf).getOrCreate()
//
//      val lines = ss.sparkContext.textFile("/Users/chailei/antu/bioqas/stattest.txt")
//
//      val obser = lines.map(_.split(",")).map(p => Vectors.dense(p(1).toDouble,
//        p(2).toDouble,p(3).toDouble,p(4).toDouble))
//
//      val summer = Statistics.colStats(obser)
//
//      println("最大值："+summer.max)
//      println("最小值"+summer.min)
//      println("方差"+summer.variance)
//      println("均值"+summer.mean)
//
//      }
//
//    })

    JdbcDialects.registerDialect(OracleDialect)
        var id= 0
        val message = kafkaStream.mapPartitions(partition => {
          partition.map(value => {
            implicit val formats: DefaultFormats.type = DefaultFormats
            val json = parse(value.value())
            //id = record.after.ID
            //            record
            json.extract[Record]

          })
        }).cache().foreachRDD( rdd => {
            if(!rdd.isEmpty()){
              var idd = 786
//              var ids = new StringBuffer()
//              var iddd = rdd.map(line => {
//                line.after.ID
//              }).collect()
////              println(idd.collect)
//              for(i <- iddd){
//                println(i)
//
//              }
//              rdd.foreachPartition(partition => {


//              println(" partition = " + rdd.getNumPartitions)
              val ss = SparkSession.builder().config(conf).getOrCreate()
              ss.sqlContext.udf.register("middleValue",new MiddleValueUDAF())

              import ss.implicits._

//              val tUserDF = ss.read.format("jdbc").options(Map(
//                "url" -> "jdbc:oracle:thin:@192.168.8.131:1521:orcl",
//                "user" -> "bioqas",
//                "password" -> "bioqas",
//                "dbtable" -> s"(select * from TP_E_BD_REPORT_DATE where id = $idd ) a",
//                "driver" -> "oracle.jdbc.driver.OracleDriver"
//              )).load()
//              tUserDF.printSchema()
//              tUserDF.show()



              val kafkaDF = rdd.map(_.after).toDF()

//              kafkaDF.printSchema()
//              kafkaDF.show()

//              val allDF = tUserDF.union(kafkaDF)
                val resultDF = kafkaDF.distinct().cache()
//              val resultDF = allDF.distinct()
              resultDF.createOrReplaceTempView("all_table")
//              resultDF.show(30)


              ss.sql("select mid,sd,ma,mi,mn,sd/mn from (select middleValue(REPORT_DATA) as mid,stddev(REPORT_DATA) as sd," +
                "max(REPORT_DATA) as ma,min(REPORT_DATA) as mi,mean(REPORT_DATA) as mn " +
                "from " +
                "all_table group by SPEC_ID,ITEM_ID,BATCH_ID)").show(30)
              ss.sql("select mid,sd,ma,mi,mn,sd/mn from (select middleValue(REPORT_DATA) as mid,stddev(REPORT_DATA) as sd," +
                "max(REPORT_DATA) as ma,min(REPORT_DATA) as mi,mean(REPORT_DATA) as mn " +
                "from " +
                "all_table group by SPEC_ID,ITEM_ID,LAB_INS_ID)").show(30)
              ss.sql("select mid,sd,ma,mi,mn,sd/mn from (select middleValue(REPORT_DATA) as mid,stddev(REPORT_DATA) as sd," +
                "max(REPORT_DATA) as ma,min(REPORT_DATA) as mi,mean(REPORT_DATA) as mn " +
                "from " +
                "all_table group by SPEC_ID,ITEM_ID,METHOD_ID)").show(30)

              ss.sql("select mid,sd,ma,mi,mn,sd/mn from (select middleValue(REPORT_DATA) as mid,stddev(REPORT_DATA) as sd," +
                "max(REPORT_DATA) as ma,min(REPORT_DATA) as mi,mean(REPORT_DATA) as mn " +
                "from " +
                "all_table group by SPEC_ID,ITEM_ID,REAGENT_NAME_ID)").show(30)

//
//              })
//              resultDF.show(1)
            }
          })

//    message.foreachRDD(rdd =>{
//
//    })

//    message.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()


  }

}
