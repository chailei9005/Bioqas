package com.bioqas.test

import java.sql.Connection

import com.bioqas.utils._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.phoenix.spark._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * 统计 计划_专业_次数_项目 到这个
  */
object BiqasAppOptimize {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("TestApp")
      .setMaster("local[5]")
      .set("spark.streaming.kafka.maxRatePerPartition","1")
    //      .set("spark.executor.extraJavaOptions", "-XX:+UseConcMarkSweepGC")
    //      .set("spark.streaming.kafka.maxRatePerPartition", "1000")
//          .set("spark.default.parallelism", "400")
//      .set("spark.sql.shuffle.partitions","400")
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
    val ssc = new StreamingContext(conf, Seconds(120))
    val noGroupNumber = "1111"
    val noGroupNumberP2sd = "1122"
    val noGroupNumberP3sd = "1133"
    val noGroupNumberWen = "1144"
    val calBrandsId = "2211"
    val calBrandsIdP2sd = "2222"
    val calBrandsIdP3sd = "2233"
    val labId = "3311"
    val labIdP2sd = "3322"
    val labIdP3sd = "3333"
    val methodId = "4411"
    val methodIdP2sd = "4422"
    val methodIdP3sd = "4433"
    val reagentId = "5511"
    val reagentIdP2sd = "5522"
    val reagentIdP3sd = "5533"
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
    JdbcDialects.registerDialect(OracleDialect)
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
//              println("********partition hou***********")
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
          val phoenixDF = ss.sqlContext.phoenixTableAsDataFrame(
            "TP_E_BD_REPORT_DATE_BUCKET",    //TP_E_BD_REPORT_DATE_BUCKET
            Seq("ID","CENTER_ID","PLAN_ID","SPEC_ID","ITEM_ID","BATCH_ID","TIMES_ID",
              "CAL_MATERIAL_BRANDS_ID","METHOD_ID","LAB_ID","LAB_INS_ID","REAGENT_NAME_ID","REPORT_DATA"),// 字段必须是大写
            Some(s"${id}"),//Some(s"ID like \'${biaoshi}${id}\'"),
            Some("biaoyuan01:2181")
          )
          phoenixDF.printSchema()
          ss.sqlContext.udf.register("middleValue",new MiddleValueUDAF())
          ss.sqlContext.udf.register("AMean",new AlgorithmAMeanValueUDAF())
          ss.sqlContext.udf.register("ASD",new AlgorithmASDValueUDAF())
          ss.sqlContext.udf.register("MCV",new AlgorithmMedSIRCVValueUDAF())
          ss.sqlContext.udf.register("MmiddleValue",new AlgorithmMedSIRMiddleValueUDAF())
          val dataDF = phoenixDF.filter("REPORT_DATA!=999d")
            .select("ID","CENTER_ID","PLAN_ID","SPEC_ID","ITEM_ID","TIMES_ID","BATCH_ID",
              "CAL_MATERIAL_BRANDS_ID","METHOD_ID","LAB_ID","LAB_INS_ID","REAGENT_NAME_ID","REPORT_DATA").cache()


//          println("total = " + dataDF.count())


          /*************************原始数据不分组****************************/
          dataDF.createOrReplaceTempView("tp")
//
//          // 如果有nan值，保存到oracle报错 java.sql.BatchUpdateException: 内部错误: Overflow Exception trying to bind NaN
          val resultDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID) as cn" +
            ",middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) end as cv" +
            ",max(REPORT_DATA) as ma,min(REPORT_DATA) as mi from tp group by CENTER_ID,PLAN_ID,SPEC_ID," +
            "ITEM_ID,TIMES_ID,BATCH_ID")

          resultDF.cache()
//
//          resultDF.printSchema()

//          resultDF.show()
          //          root
          //          |-- CENTER_ID: integer (nullable = true)
          //          |-- PLAN_ID: integer (nullable = true)
          //          |-- SPEC_ID: integer (nullable = true)
          //          |-- ITEM_ID: integer (nullable = true)
          //          |-- TIMES_ID: string (nullable = true)
          //          |-- BATCH_ID: integer (nullable = true)
          //          |-- cn: long (nullable = false)
          //          |-- max(REPORT_DATA): double (nullable = true)
          //          |-- min(REPORT_DATA): double (nullable = true)
          //          |-- middle: double (nullable = true)
          //          |-- mn: double (nullable = true)
          //          |-- sd: double (nullable = true)
          //          |-- cv: double (nullable = true)

          import  ss.implicits._
          val resultDFTransfer = resultDF.rdd.map(value => {

            val pk_id: String =  value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + noGroupNumber

            (value.getAs[Integer](0),value.getAs[Integer](1),value.getAs[Integer](2),
              value.getAs[Integer](3),value.getAs[Integer](4),value.getAs[Integer](5),
              value.getAs[Long](6),value.getAs[Double](7),value.getAs[Double](8),
              value.getAs[Double](9),value.getAs[Double](10),value.getAs[Double](11),value.getAs[Double](12),
              0,0,pk_id)
          }).toDF

//          root
//          |-- _1: integer (nullable = true)
//          |-- _2: integer (nullable = true)
//          |-- _3: integer (nullable = true)
//          |-- _4: integer (nullable = true)
//          |-- _5: string (nullable = true)
//          |-- _6: integer (nullable = true)
//          |-- _7: long (nullable = true)
//          |-- _8: double (nullable = true)
//          |-- _9: double (nullable = true)
//          |-- _10: double (nullable = true)
//          |-- _11: double (nullable = true)
//          |-- _12: double (nullable = true)
//          |-- _13: integer (nullable = true)
//          |-- _14: integer (nullable = true)


          resultDF.createOrReplaceTempView("result")
//
//          //          ss.sql("cache table result_cache as select * from result")
//
          val totalDF = ss.sql("select tt.*,tp.REPORT_DATA from result tt join tp on " +
            "tp.CENTER_ID = tt.CENTER_ID and tp.PLAN_ID = tt.PLAN_ID and tp.SPEC_ID = tt.SPEC_ID and " +
            "tp.ITEM_ID = tt.ITEM_ID and tp.TIMES_ID = tt.TIMES_ID " +
            "and tp.BATCH_ID = tt.BATCH_ID")
//
//          //          ss.sql("uncache table result_cache")
//
//          totalDF.show()
//
          totalDF.createOrReplaceTempView("total")

          val p3sdDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID),middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) end as cv,max(REPORT_DATA),min(REPORT_DATA) from total where " +
            "case when isnan(sd) then REPORT_DATA >= REPORT_DATA and REPORT_DATA <= REPORT_DATA " +
            "else REPORT_DATA >= (mn-3*sd) and REPORT_DATA <= (mn + 3*sd) end group by CENTER_ID," +
            "PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID")

//          p3sdDF.show()

          val p3sdDFTransfer = p3sdDF.rdd.map(value => {

            val pk_id: String =  value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + noGroupNumberP3sd

            (value.getAs[Integer](0),value.getAs[Integer](1),value.getAs[Integer](2),
              value.getAs[Integer](3),value.getAs[Integer](4),value.getAs[Integer](5),
              value.getAs[Long](6),value.getAs[Double](7),value.getAs[Double](8),
              value.getAs[Double](9),value.getAs[Double](10),value.getAs[Double](11),value.getAs[Double](12),
              0,0,pk_id)
          }).toDF


          val p2sdDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID),middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) end as cv,max(REPORT_DATA),min(REPORT_DATA) from total where " +
            "case when isnan(sd) then REPORT_DATA >= REPORT_DATA and REPORT_DATA <= REPORT_DATA " +
            "else REPORT_DATA >= (mn-2*sd) and REPORT_DATA <= (mn + 2*sd) end group by CENTER_ID," +
            "PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID")

//          p2sdDF.show()


          val p2sdDFTransfer = p3sdDF.rdd.map(value => {

            val pk_id: String =  value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + noGroupNumberP2sd

            (value.getAs[Integer](0),value.getAs[Integer](1),value.getAs[Integer](2),
              value.getAs[Integer](3),value.getAs[Integer](4),value.getAs[Integer](5),
              value.getAs[Long](6),value.getAs[Double](7),value.getAs[Double](8),
              value.getAs[Double](9),value.getAs[Double](10),value.getAs[Double](11),value.getAs[Double](12),
              0,0,pk_id)
          }).toDF

          /**************************校准物厂商***************************/

          val resultCALDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID)" +
            ",middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) end as cv," +
            "max(REPORT_DATA),min(REPORT_DATA),CAL_MATERIAL_BRANDS_ID from tp group by CENTER_ID,PLAN_ID,SPEC_ID," +
            "ITEM_ID,TIMES_ID,BATCH_ID,CAL_MATERIAL_BRANDS_ID")

          val resultCALDFTransfer = resultCALDF.rdd.map(value => {

            val pk_id: String =  value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + value.getAs[Integer](13) + calBrandsId

            (value.getAs[Integer](0),value.getAs[Integer](1),value.getAs[Integer](2),
              value.getAs[Integer](3),value.getAs[Integer](4),value.getAs[Integer](5),
              value.getAs[Long](6),value.getAs[Double](7),value.getAs[Double](8),
              value.getAs[Double](9),value.getAs[Double](10),value.getAs[Double](11),value.getAs[Double](12),
              1,value.getAs[Integer](13),pk_id)
          }).toDF


          resultCALDF.createOrReplaceTempView("resultCAL")

          //          ss.sql("cache table resultCAL_cache as select * from resultCAL")

          val totalCALDF = ss.sql("select tt.*,tp.REPORT_DATA from resultCAL tt join tp on " +
            "tp.CENTER_ID = tt.CENTER_ID and tp.PLAN_ID = tt.PLAN_ID and tp.SPEC_ID = tt.SPEC_ID and " +
            "tp.ITEM_ID = tt.ITEM_ID and tp.TIMES_ID = tt.TIMES_ID " +
            "and tp.BATCH_ID = tt.BATCH_ID and tp.CAL_MATERIAL_BRANDS_ID=tt.CAL_MATERIAL_BRANDS_ID")

          //          ss.sql("uncache table resultCAL_cache")

          totalCALDF.createOrReplaceTempView("totalCAL")

          val p3sdCALDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID),middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) end as cv,max(REPORT_DATA),min(REPORT_DATA),CAL_MATERIAL_BRANDS_ID from totalCAL where " +
            "case when isnan(sd) then REPORT_DATA >= REPORT_DATA and REPORT_DATA <= REPORT_DATA " +
            "else REPORT_DATA >= (mn-3*sd) and REPORT_DATA <= (mn + 3*sd) end group by CENTER_ID," +
            "PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,CAL_MATERIAL_BRANDS_ID")


          val p3sdCALDFTransfer = p3sdCALDF.rdd.map(value => {

            val pk_id: String =  value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + value.getAs[Integer](13) + calBrandsIdP3sd

            (value.getAs[Integer](0),value.getAs[Integer](1),value.getAs[Integer](2),
              value.getAs[Integer](3),value.getAs[Integer](4),value.getAs[Integer](5),
              value.getAs[Long](6),value.getAs[Double](7),value.getAs[Double](8),
              value.getAs[Double](9),value.getAs[Double](10),value.getAs[Double](11),value.getAs[Double](12),
              1,value.getAs[Integer](13),pk_id)
          }).toDF



          val p2sdCALDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID),middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) end as cv,max(REPORT_DATA),min(REPORT_DATA),CAL_MATERIAL_BRANDS_ID from totalCAL where " +
            "case when isnan(sd) then REPORT_DATA >= REPORT_DATA and REPORT_DATA <= REPORT_DATA " +
            "else REPORT_DATA >= (mn-2*sd) and REPORT_DATA <= (mn + 2*sd) end group by CENTER_ID," +
            "PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,CAL_MATERIAL_BRANDS_ID")

          val p2sdCALDFTransfer = p2sdCALDF.rdd.map(value => {

            val pk_id: String =  value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + value.getAs[Integer](13) + calBrandsIdP2sd

            (value.getAs[Integer](0),value.getAs[Integer](1),value.getAs[Integer](2),
              value.getAs[Integer](3),value.getAs[Integer](4),value.getAs[Integer](5),
              value.getAs[Long](6),value.getAs[Double](7),value.getAs[Double](8),
              value.getAs[Double](9),value.getAs[Double](10),value.getAs[Double](11),value.getAs[Double](12),
              1,value.getAs[Integer](13),pk_id)
          }).toDF



          /**************************仪器***************************/


          val resultLabDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID)" +
            ",middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) end as cv,max(REPORT_DATA),min(REPORT_DATA),LAB_ID from tp group by CENTER_ID,PLAN_ID,SPEC_ID," +
            "ITEM_ID,TIMES_ID,BATCH_ID,LAB_ID")

          val resultLabDFTransfer = resultLabDF.rdd.map(value => {

            val pk_id: String =  value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + value.getAs[Integer](13) + labId

            (value.getAs[Integer](0),value.getAs[Integer](1),value.getAs[Integer](2),
              value.getAs[Integer](3),value.getAs[Integer](4),value.getAs[Integer](5),
              value.getAs[Long](6),value.getAs[Double](7),value.getAs[Double](8),
              value.getAs[Double](9),value.getAs[Double](10),value.getAs[Double](11),value.getAs[Double](12),
              2,value.getAs[Integer](13),pk_id)
          }).toDF


          resultLabDF.createOrReplaceTempView("resultLab")

          //          ss.sql("cache table resultLab_cache as select * from resultLab")

          val totalLabDF = ss.sql("select tt.*,tp.REPORT_DATA from resultLab tt join tp on " +
            "tp.CENTER_ID = tt.CENTER_ID and tp.PLAN_ID = tt.PLAN_ID and tp.SPEC_ID = tt.SPEC_ID and " +
            "tp.ITEM_ID = tt.ITEM_ID and tp.TIMES_ID = tt.TIMES_ID " +
            "and tp.BATCH_ID = tt.BATCH_ID and tp.LAB_ID=tt.LAB_ID")


          totalLabDF.createOrReplaceTempView("totalLab")

          val p3sdLabDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID),middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) end as cv,max(REPORT_DATA),min(REPORT_DATA),LAB_ID from totalLab where " +
            "case when isnan(sd) then REPORT_DATA >= REPORT_DATA and REPORT_DATA <= REPORT_DATA " +
            "else REPORT_DATA >= (mn-3*sd) and REPORT_DATA <= (mn + 3*sd) end group by CENTER_ID," +
            "PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,LAB_ID")

          val p3sdLabDFTransfer = p3sdLabDF.rdd.map(value => {

            val pk_id: String =  value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + value.getAs[Integer](13) + labIdP3sd

            (value.getAs[Integer](0),value.getAs[Integer](1),value.getAs[Integer](2),
              value.getAs[Integer](3),value.getAs[Integer](4),value.getAs[Integer](5),
              value.getAs[Long](6),value.getAs[Double](7),value.getAs[Double](8),
              value.getAs[Double](9),value.getAs[Double](10),value.getAs[Double](11),value.getAs[Double](12),
              2,value.getAs[Integer](13),pk_id)
          }).toDF



          val p2sdLabDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID),middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) end as cv,max(REPORT_DATA),min(REPORT_DATA),LAB_ID from totalLab where " +
            "case when isnan(sd) then REPORT_DATA >= REPORT_DATA and REPORT_DATA <= REPORT_DATA " +
            "else REPORT_DATA >= (mn-2*sd) and REPORT_DATA <= (mn + 2*sd) end group by CENTER_ID," +
            "PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,LAB_ID")

          val p2sdLabDFTransfer = p2sdLabDF.rdd.map(value => {

            val pk_id: String =  value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + value.getAs[Integer](13) + labIdP2sd

            (value.getAs[Integer](0),value.getAs[Integer](1),value.getAs[Integer](2),
              value.getAs[Integer](3),value.getAs[Integer](4),value.getAs[Integer](5),
              value.getAs[Long](6),value.getAs[Double](7),value.getAs[Double](8),
              value.getAs[Double](9),value.getAs[Double](10),value.getAs[Double](11),value.getAs[Double](12),
              2,value.getAs[Integer](13),pk_id)
          }).toDF



          /*****************************方法************************/

          val resultMethodDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID)" +
            ",middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) end as cv,max(REPORT_DATA),min(REPORT_DATA),METHOD_ID from tp group by CENTER_ID,PLAN_ID,SPEC_ID," +
            "ITEM_ID,TIMES_ID,BATCH_ID,METHOD_ID")

          val resultMethodDFTransfer = resultMethodDF.rdd.map(value => {

            val pk_id: String =  value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + value.getAs[Integer](13) + methodId

            (value.getAs[Integer](0),value.getAs[Integer](1),value.getAs[Integer](2),
              value.getAs[Integer](3),value.getAs[Integer](4),value.getAs[Integer](5),
              value.getAs[Long](6),value.getAs[Double](7),value.getAs[Double](8),
              value.getAs[Double](9),value.getAs[Double](10),value.getAs[Double](11),value.getAs[Double](12),
              3,value.getAs[Integer](13),pk_id)
          }).toDF


          resultMethodDF.createOrReplaceTempView("resultMethod")

          //          ss.sql("cache table resultMethod_cache as select * from resultMethod")

          val totalMethodDF = ss.sql("select tt.*,tp.REPORT_DATA from resultMethod tt join tp on " +
            "tp.CENTER_ID = tt.CENTER_ID and tp.PLAN_ID = tt.PLAN_ID and tp.SPEC_ID = tt.SPEC_ID and " +
            "tp.ITEM_ID = tt.ITEM_ID and tp.TIMES_ID = tt.TIMES_ID " +
            "and tp.BATCH_ID = tt.BATCH_ID and tp.METHOD_ID=tt.METHOD_ID")

          //          ss.sql("uncache table resultMethod_cache")

          totalMethodDF.createOrReplaceTempView("totalMethod")

          val p3sdMethodDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID),middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) end as cv,max(REPORT_DATA),min(REPORT_DATA),METHOD_ID from totalMethod where " +
            "case when isnan(sd) then REPORT_DATA >= REPORT_DATA and REPORT_DATA <= REPORT_DATA " +
            "else REPORT_DATA >= (mn-3*sd) and REPORT_DATA <= (mn + 3*sd) end group by CENTER_ID," +
            "PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,METHOD_ID")


          val p3sdMethodDFTransfer = p3sdMethodDF.rdd.map(value => {

            val pk_id: String =  value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + value.getAs[Integer](13) + methodIdP3sd

            (value.getAs[Integer](0),value.getAs[Integer](1),value.getAs[Integer](2),
              value.getAs[Integer](3),value.getAs[Integer](4),value.getAs[Integer](5),
              value.getAs[Long](6),value.getAs[Double](7),value.getAs[Double](8),
              value.getAs[Double](9),value.getAs[Double](10),value.getAs[Double](11),value.getAs[Double](12),
              3,value.getAs[Integer](13),pk_id)
          }).toDF



          val p2sdMethodDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID),middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) end as cv,max(REPORT_DATA),min(REPORT_DATA),METHOD_ID from totalMethod where " +
            "case when isnan(sd) then REPORT_DATA >= REPORT_DATA and REPORT_DATA <= REPORT_DATA " +
            "else REPORT_DATA >= (mn-2*sd) and REPORT_DATA <= (mn + 2*sd) end group by CENTER_ID," +
            "PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,METHOD_ID")

          val p2sdMethodDFTransfer = p2sdMethodDF.rdd.map(value => {

            val pk_id: String =  value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + value.getAs[Integer](13) + methodIdP2sd

            (value.getAs[Integer](0),value.getAs[Integer](1),value.getAs[Integer](2),
              value.getAs[Integer](3),value.getAs[Integer](4),value.getAs[Integer](5),
              value.getAs[Long](6),value.getAs[Double](7),value.getAs[Double](8),
              value.getAs[Double](9),value.getAs[Double](10),value.getAs[Double](11),value.getAs[Double](12),
              3,value.getAs[Integer](13),pk_id)
          }).toDF



          /*****************************试剂************************/


          val resultReagentDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID)" +
            ",middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) end as cv,max(REPORT_DATA),min(REPORT_DATA),REAGENT_NAME_ID from tp group by CENTER_ID,PLAN_ID,SPEC_ID," +
            "ITEM_ID,TIMES_ID,BATCH_ID,REAGENT_NAME_ID")

          val resultReagentDFTransfer = resultReagentDF.rdd.map(value => {

            val pk_id: String =  value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + value.getAs[Integer](13) + reagentId

            (value.getAs[Integer](0),value.getAs[Integer](1),value.getAs[Integer](2),
              value.getAs[Integer](3),value.getAs[Integer](4),value.getAs[Integer](5),
              value.getAs[Long](6),value.getAs[Double](7),value.getAs[Double](8),
              value.getAs[Double](9),value.getAs[Double](10),value.getAs[Double](11),value.getAs[Double](12),
              4,value.getAs[Integer](13),pk_id)
          }).toDF



          resultReagentDF.createOrReplaceTempView("resultReagent")

          //          ss.sql("cache table resultReagent_cache as select * from resultReagent")

          val totalReagentDF = ss.sql("select tt.*,tp.REPORT_DATA from resultReagent tt join tp on " +
            "tp.CENTER_ID = tt.CENTER_ID and tp.PLAN_ID = tt.PLAN_ID and tp.SPEC_ID = tt.SPEC_ID and " +
            "tp.ITEM_ID = tt.ITEM_ID and tp.TIMES_ID = tt.TIMES_ID " +
            "and tp.BATCH_ID = tt.BATCH_ID and tp.REAGENT_NAME_ID=tt.REAGENT_NAME_ID")

          //          ss.sql("uncache table resultReagent_cache")


          totalReagentDF.createOrReplaceTempView("totalReagent")

          val p3sdReagentDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID),middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) end as cv,max(REPORT_DATA),min(REPORT_DATA),REAGENT_NAME_ID from totalReagent where " +
            "case when isnan(sd) then REPORT_DATA >= REPORT_DATA and REPORT_DATA <= REPORT_DATA " +
            "else REPORT_DATA >= (mn-3*sd) and REPORT_DATA <= (mn + 3*sd) end group by CENTER_ID," +
            "PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,REAGENT_NAME_ID")

          val p3sdReagentDFTransfer = p3sdReagentDF.rdd.map(value => {

            val pk_id: String =  value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + value.getAs[Integer](13) + reagentIdP3sd

            (value.getAs[Integer](0),value.getAs[Integer](1),value.getAs[Integer](2),
              value.getAs[Integer](3),value.getAs[Integer](4),value.getAs[Integer](5),
              value.getAs[Long](6),value.getAs[Double](7),value.getAs[Double](8),
              value.getAs[Double](9),value.getAs[Double](10),value.getAs[Double](11),value.getAs[Double](12),
              4,value.getAs[Integer](13),pk_id)
          }).toDF


          val p2sdReagentDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID),middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) end as cv,max(REPORT_DATA),min(REPORT_DATA),REAGENT_NAME_ID from totalReagent where " +
            "case when isnan(sd) then REPORT_DATA >= REPORT_DATA and REPORT_DATA <= REPORT_DATA " +
            "else REPORT_DATA >= (mn-2*sd) and REPORT_DATA <= (mn + 2*sd) end group by CENTER_ID," +
            "PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,REAGENT_NAME_ID")

          val p2sdReagentDFTransfer = p2sdReagentDF.rdd.map(value => {

            val pk_id: String =  value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + value.getAs[Integer](13) + reagentIdP2sd

            (value.getAs[Integer](0),value.getAs[Integer](1),value.getAs[Integer](2),
              value.getAs[Integer](3),value.getAs[Integer](4),value.getAs[Integer](5),
              value.getAs[Long](6),value.getAs[Double](7),value.getAs[Double](8),
              value.getAs[Double](9),value.getAs[Double](10),value.getAs[Double](11),value.getAs[Double](12),
              4,value.getAs[Integer](13),pk_id)
          }).toDF

//          ss.sqlContext.udf.register("AMean",new AlgorithmAMeanValueUDAF())
//          ss.sqlContext.udf.register("ASD",new AlgorithmASDValueUDAF())
//          ss.sqlContext.udf.register("MCV",new AlgorithmMedSIRCVValueUDAF())
//          ss.sqlContext.udf.register("MmiddleValue",new AlgorithmMedSIRMiddleValueUDAF())

          // 稳健统计
          val wenDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID),MmiddleValue(REPORT_DATA) as middle,AMean(REPORT_DATA) as mn," +
            "case when isnan(ASD(REPORT_DATA)) then 0 else ASD(REPORT_DATA) end as sd," +
            "case when isnan(MCV(REPORT_DATA)) then 0 else MCV(REPORT_DATA) end as cv," +
            "max(REPORT_DATA) as ma,min(REPORT_DATA) as mi from tp group by CENTER_ID,PLAN_ID,SPEC_ID," +
            "ITEM_ID,TIMES_ID,BATCH_ID")

//          wenDF.show()


          val wenDFTransfer = wenDF.rdd.map(value => {

            val pk_id: String =  value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + noGroupNumberWen

            (value.getAs[Integer](0),value.getAs[Integer](1),value.getAs[Integer](2),
              value.getAs[Integer](3),value.getAs[Integer](4),value.getAs[Integer](5),
              value.getAs[Long](6),value.getAs[Double](7),value.getAs[Double](8),
              value.getAs[Double](9),value.getAs[Double](10),value.getAs[Double](11),value.getAs[Double](12),
              5,2,pk_id)
          }).toDF


//        wenDFTransfer.show()


          val allDF = wenDFTransfer.union(resultDFTransfer).union(p3sdDFTransfer).union(p2sdDFTransfer)
                      .union(resultCALDFTransfer).union(p3sdCALDFTransfer).union(p2sdCALDFTransfer)
                      .union(resultMethodDFTransfer).union(p3sdMethodDFTransfer).union(p2sdMethodDFTransfer)
                      .union(resultLabDFTransfer).union(p3sdLabDFTransfer).union(p2sdLabDFTransfer)
                      .union(resultReagentDFTransfer).union(p3sdReagentDFTransfer).union(p2sdReagentDFTransfer)

//          allDF.show()
//          println(allDF.count())
          allDF.repartition(32).foreachPartition(rows => {

            val connect: Connection = OracleUtilsOptimize.getConnect()


//            val pt = connect.prepareStatement(OracleUtilsOptimize.querySql)
//
//
//            val updatePt = connect.prepareStatement(OracleUtilsOptimize.updateSql)
//
            val pstmt = connect.prepareStatement(OracleUtilsOptimize.sql)

            connect.setAutoCommit(false)
            rows.foreach( value => {


//              val pk_id: String =  value.getAs[Integer](0) + "" +
//                value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
//                value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
//                value.getAs[Integer](5) + "" + value.getAs[Integer](13) + "5" + "3"

//              val pk_id = value.getAs[String](15)
//
//              if(OracleUtilsOptimize.queryRecord(pt,pk_id)){
//
//                OracleUtilsOptimize.updateBatch(updatePt,value,pk_id)
//
//              }else{

                //TODO groupid grouptype
//                OracleUtilsOptimize.insertBatch(pstmt,value,pk_id,4)

//              }

            })
//
//            updatePt.executeBatch()
//            pstmt.executeBatch()

            connect.commit()
            OracleUtilsOptimize.close(connect,pstmt)
//            OracleUtilsOptimize.close(connect,updatePt)
//            OracleUtilsOptimize.close(connect,pt)

          })


        }
        accu.reset()
        myAcc.reset()
        kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRangs)

      }


    })

    println("================================")



    ssc.start()
    ssc.awaitTermination()




  }
}
