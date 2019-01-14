package com.bioqas.test

import java.util

import com.bioqas.utils.{JSONParseUtil, PhoenixUtils, IdAccumulator}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

import scala.collection.mutable.ArrayBuffer


// phoenixTableAsDataFrame
import scala.collection.mutable
import org.apache.spark.sql.functions._

/**
  * Created by chailei on 18/7/12.
  */
object OracleToHbaseCompute {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("TestApp")
      .setMaster("local[4]")
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
    val topics = "bioqas"
    val topicSet = topics.split(",")
    val ssc = new StreamingContext(conf, Seconds(60))

    val kafkaStream = KafkaUtils
      .createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicSet, paramMap)
    )

    val accu = ssc.sparkContext.longAccumulator("myflag")
    val rddaccu = ssc.sparkContext.longAccumulator("count")
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


            println("------------create phoenixutil-------------------------")
            val phoenixUtil = new PhoenixUtils()

            var batchSize = 0
            val commitSize = 1000
            var flag = 0

            partitions.foreach(line => {
              accu.add(1l)
              flag = 1
              println("********partition hou***********")
              idBroadCast.value.add("chailei"+accu.value)
              val value = line.value()

              //          println(value)
              val sql = JSONParseUtil.parseJSONToPhoenixSQL(value.trim)
              myAcc.add(sql._2)
              phoenixUtil.saveToHbase(sql._1)

              batchSize = batchSize + 1

              if (batchSize % commitSize == 0) {
                phoenixUtil.conn.commit()
              }


            })
            println("------------last commit and close connection-------------------------")
            phoenixUtil.closeConn()


            println("flag = " + flag)
            if (flag == 4) {
              val resultPhoenix = new PhoenixUtils()

              //  val result = resultPhoenix.searchFromHbase("select * from TP_E_BD_REPORT_DATE_BAK where center_id = 22")
              val ss = SparkSession.builder().config(conf).getOrCreate()
              val phoenixDF = ss.read.format("org.apache.phoenix.spark")
                .option("table" , "TP_E_BD_REPORT_DATE_BAK")
                .option( "zkUrl" , "biaoyuan01:2181")
                .load()
              flag = 0
              phoenixDF.printSchema()



              println(phoenixDF.count())
              resultPhoenix.closeConn()
            }


          }
        })
        println("accu = " + accu.value)
        val id = mutable.StringBuilder.newBuilder
        for(x:String <- myAcc.value.split(",").toSet){// 这里不加String类型会报错
          id.append("id like ").append("\'").append("%").append(x).append("%").append("\'").append(" or ")
        }
        id.delete(id.length-4,id.length-1)
        println("id " + id.toString())
        println("myAcc " + myAcc.value + "   " + myAcc.value.equals(""))
        println("broadcast = " + idBroadCast.value.size)
        //        if(idBroadCast.value.size != 0){
        if(!myAcc.value.equals("")){// 当没有数据的时候，不读取数据进行计算
        val ss = SparkSession.builder().config(conf).getOrCreate()
          //          val phoenixDF = ss.read.format("org.apache.phoenix.spark")
          //            .option("table" , "TP_E_BD_REPORT_DATE_BUCKET")
          //            .option( "zkUrl" , "biaoyuan01:2181")
          //            .load()
          val biaoshi = "%"
          ss.sqlContext.udf.register("middleValue",new MiddleValueUDAF())
          val phoenixDF = ss.sqlContext.phoenixTableAsDataFrame(
            "TP_E_BD_REPORT_DATE_BUCKET",    //TP_E_BD_REPORT_DATE_BUCKET
            Seq("ID","CENTER_ID","PLAN_ID","SPEC_ID","ITEM_ID","BATCH_ID","TIMES_NAME",
              "CAL_MATERIAL_BRANDS_ID","METHOD_ID","LAB_INS_ID","REAGENT_NAME_ID","REPORT_DATA"),// 字段必须是大写
            Some(s"${id}"),//Some(s"ID like \'${biaoshi}${id}\'"),
            Some("biaoyuan01:2181")
          ).cache()
          phoenixDF.printSchema()
          ss.sqlContext.udf.register("middleValue",new MiddleValueUDAF())
          val dataDF = phoenixDF.filter("REPORT_DATA!=999d")
            .select("ID","CENTER_ID","PLAN_ID","SPEC_ID","ITEM_ID","TIMES_NAME","BATCH_ID",
              "CAL_MATERIAL_BRANDS_ID","METHOD_ID","LAB_INS_ID","REAGENT_NAME_ID","REPORT_DATA")
            .cache()

//          println("total = " + dataDF.count())


          /*************************原始数据不分组****************************/

          dataDF.createOrReplaceTempView("tp")

          val df = ss.sql("select tt.*,tp.REPORT_DATA from (select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,METHOD_ID," +
            "TIMES_NAME,BATCH_ID,count(BATCH_ID),mean(REPORT_DATA) as mn,middleValue(REPORT_DATA) as middle,stddev(REPORT_DATA) as sd from tp " +
            "group by CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,METHOD_ID,TIMES_NAME,BATCH_ID) tt join tp on " +
            "tp.CENTER_ID = tt.CENTER_ID and tp.PLAN_ID = tt.PLAN_ID and tp.SPEC_ID = tt.SPEC_ID and " +
            "tp.ITEM_ID = tt.ITEM_ID and tp.METHOD_ID = tt.METHOD_ID and tp.TIMES_NAME = tt.TIMES_NAME " +
            "and tp.BATCH_ID = tt.BATCH_ID").cache()


          df.createOrReplaceTempView("compute")
          df.show()

          val p_3sd = ss.sql("select * from compute where mn <= (mn + 3*sd) and mn >= (mn - 3*sd)")

          val p_2sd = ss.sql("select * from compute where mn <= (mn + 2*sd) and mn >= (mn - 2*sd)")

          val total = p_2sd.union(p_3sd)

          total.createOrReplaceTempView("total")
          println("total = " + total.count())
          //CENTER_ID|PLAN_ID|SPEC_ID|ITEM_ID|METHOD_ID|TIMES_NAME|BATCH_ID|
          val mmDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,METHOD_ID,TIMES_NAME,BATCH_ID,count(BATCH_ID)" +
            ",max(mn),min(mn),max(middle),min(middle) " +
            "from total group by CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,METHOD_ID,TIMES_NAME,BATCH_ID")
          import ss.implicits._

          val mmRdd = mmDF.rdd.map(line => {
            val array = new ArrayBuffer[Double]()
            array.append(line.getAs[Double](8))
            array.append(line.getAs[Double](9))
            array.append(line.getAs[Double](10))
            array.append(line.getAs[Double](11))
            val max = array.max
            val min = array.min
            (line.getAs[Int](0),line.getAs[Int](1),line.getAs[Int](2),line.getAs[Int](3)
              ,line.getAs[Int](4),line.getAs[String](5),line.getAs[Int](6),max.formatted("%.2f"),min.formatted("%.2f"))
          }).toDF("CENTER_ID","PLAN_ID","SPEC_ID","ITEM_ID","METHOD_ID","TIMES_NAME","BATCH_ID","max","min")


          println("======================================")
//          mmRdd.rdd.flatMap(line => {
//            (line.getAs[String](8).toDouble to line.getAs[String](7).toDouble by 0.1)
//          }).foreach(println)


          val targetRDD = mmRdd.rdd.map(line => ((line.getAs[Int](0),line.getAs[Int](1),line.getAs[Int](2),line.getAs[Int](3),
          line.getAs[Int](4),line.getAs[String](5),line.getAs[Int](6)),
            (line.getAs[String](8).toDouble to line.getAs[String](7).toDouble by 0.1)))
              .map(lines => {
                val sb = mutable.StringBuilder.newBuilder
//                val arr = new  ArrayBuffer()
                for(value <- lines._2){
                  sb.append(lines._1._1).append(",")
                    .append(lines._1._2).append(",")
                    .append(lines._1._3).append(",")
                    .append(lines._1._4).append(",")
                    .append(lines._1._5).append(",")
                    .append(lines._1._6).append(",")
                    .append(lines._1._7).append(",")
                    .append(value).append("\t")
                  rddaccu.add(1)
                }

                sb.toString()
              }).flatMap(values => {
            values.split("\t")
          }).cache()


//          targetRDD.foreach(println)


          val totalTargetDF = targetRDD.map(values => {
            val targetValues = values.split(",")
            (targetValues(0),targetValues(1),targetValues(2),
              targetValues(3),targetValues(4),targetValues(5),targetValues(6),targetValues(7))
          }).toDF("CENTER_ID","PLAN_ID","SPEC_ID","ITEM_ID","METHOD_ID","TIMES_NAME","BATCH_ID","target")

          totalTargetDF.show()
          totalTargetDF.createOrReplaceTempView("target")

          val computeDF = ss.sql("select target.*,tp.REPORT_DATA from target join tp on " +
                "tp.CENTER_ID = target.CENTER_ID and tp.PLAN_ID = target.PLAN_ID and tp.SPEC_ID = target.SPEC_ID and " +
                "tp.ITEM_ID = target.ITEM_ID and tp.METHOD_ID = target.METHOD_ID and tp.TIMES_NAME = target.TIMES_NAME " +
                "and tp.BATCH_ID = target.BATCH_ID")

          computeDF.show(10)

          println("mmDF = " + mmDF.count()+ ",mmRdd = " + mmRdd.count() + ",targetRDD="+targetRDD.count()
            +",targetDF = " + totalTargetDF.count()+",computeDF=" + computeDF.count() + ",rddaccu = " + rddaccu)
          val pjbz = 10
          computeDF.printSchema()
          computeDF.rdd.map(line => {
            val report_data = line.getAs[Double](8)
            val target = line.getAs[String](7).toDouble
            var score = 0
            if(report_data >= (target - pjbz) && report_data <= (target + pjbz)){
                score = 1
            }
            (line.getAs[String](0),line.getAs[String](1),line.getAs[String](2),line.getAs[String](3),
              line.getAs[String](4),line.getAs[String](5),line.getAs[String](6),line.getAs[String](7).toDouble.formatted("%.2f"),line.getAs[Double](8),score)
          }).toDF("CENTER_ID","PLAN_ID","SPEC_ID","ITEM_ID","METHOD_ID","TIMES_NAME","BATCH_ID","target","REPORT_DATA","score")
            .show(100)

//          root
//          |-- ID: string (nullable = true)
//          |-- CENTER_ID: integer (nullable = true)
//          |-- PLAN_ID: integer (nullable = true)
//          |-- SPEC_ID: integer (nullable = true)
//          |-- ITEM_ID: integer (nullable = true)
//          |-- BATCH_ID: integer (nullable = true)
//          |-- TIMES_NAME: string (nullable = true)
//          |-- CAL_MATERIAL_BRANDS_ID: integer (nullable = true)
//          |-- METHOD_ID: integer (nullable = true)
//          |-- LAB_INS_ID: integer (nullable = true)
//          |-- REAGENT_NAME_ID: integer (nullable = true)
//          |-- REPORT_DATA: double (nullable = true)

//          total.show()

        }
        accu.reset()
        myAcc.reset()
        rddaccu.reset()
        kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRangs)

      }


    })

    println("================================")



    ssc.start()
    ssc.awaitTermination()



  }

}
