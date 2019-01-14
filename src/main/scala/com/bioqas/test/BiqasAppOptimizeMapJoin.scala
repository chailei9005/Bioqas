package com.bioqas.test

import java.sql.Connection

import com.bioqas.C3p0Utils
import com.bioqas.utils._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.phoenix.spark._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ SparkSession}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * 统计 计划_专业_次数_项目 到这个
  */
object BiqasAppOptimizeMapJoin {


  def main(args: Array[String]) {

    val conf = new SparkConf()
    //      .setAppName("TestApp")
    //      .setMaster("local[17]")
    //      .set("spark.streaming.kafka.maxRatePerPartition", "300")
    //      .set("spark.sql.shuffle.partitions", "10")
    //          "metadata.broker.list" -> "192.168.8.155:9092,192.168.8.156:9092,192.168.8.157:9092"

    // metadata.broker.list 参数为什么不能用ip:端口
    val paramMap = Map[String, Object](
      "bootstrap.servers" -> Configs.getString(Constants.KAFKA_SERVER),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "groupReportData", //test1
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "max.partition.fetch.bytes" -> (2621440: java.lang.Integer), //default: 1048576
      "request.timeout.ms" -> (200000: java.lang.Integer), //default: 60000
      "session.timeout.ms" -> (190000: java.lang.Integer) //default: 30000
    )
    val topics = "reportData"
    val topicSet = topics.split(",")
    val ssc = new StreamingContext(conf, Seconds(Configs.getInt(Constants.STREAMING_REPORT_DATA)))
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
    val insId = "6611"
    val insIdP2sd = "6622"
    val insIdP3sd = "6633"
    val kafkaStream = KafkaUtils
      .createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicSet, paramMap)
    )

    val accu = ssc.sparkContext.longAccumulator("myflag")
    // id累加器 统计输入数据的id
    val myAcc = new IdAccumulator
    ssc.sparkContext.register(myAcc, "myAcc")
    // 广播变量
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
              idBroadCast.value.add("chailei" + accu.value)
              val value = line.value()
              val sql = JSONParseUtil.parseJSONToPhoenixSQL(value.trim)
              if (!"".equals(sql._1)) {

                myAcc.add(sql._2)
                phoenixUtil.saveToHbase(sql._1)

                batchSize = batchSize + 1

                if (batchSize % commitSize == 0) {
                  phoenixUtil.conn.commit()
                }
              }


            })
            phoenixUtil.closeConn()




          }
        })
        println("accu = " + accu.value)
        val id = mutable.StringBuilder.newBuilder
        for (x: String <- myAcc.value.split(",").toSet) {
          // 这里不加String类型会报错
          id.append("id like ").append("\'").append("%").append(x).append("%").append("\'").append(" or ")
        }
        id.delete(id.length - 4, id.length - 1)
        println("id " + id.toString())
        println("myAcc " + myAcc.value + "   " + myAcc.value.equals(""))
        println("broadcast = " + idBroadCast.value.size)

        if (!myAcc.value.equals("")) {
          // 当没有数据的时候，不读取数据进行计算

          println("~~~~~~~~~~~~~~~~~~~~~~1~~~~~~~~~~~~~~~~~~~~~~~~")
          val ss = SparkSession.builder().config(conf).getOrCreate()
          //          val phoenixDF = ss.read.format("org.apache.phoenix.spark")
          //            .option("table" , "TP_E_BD_REPORT_DATE_BUCKET")
          //            .option( "zkUrl" , "biaoyuan01:2181")
          //            .load()


          val biaoshi = "%"
          val phoenixDF = ss.sqlContext.phoenixTableAsDataFrame(
            "TP_E_BD_REPORT_DATA_BUCKET", //TP_E_BD_REPORT_DATE_BUCKET
            Seq("ID", "CENTER_ID", "PLAN_ID", "SPEC_ID", "ITEM_ID", "BATCH_ID", "TIMES_ID",
              "CAL_MATERIAL_BRANDS_ID", "METHOD_ID", "INS_BRANDS_ID", "LAB_INS_ID", "REAGENT_BRANDS_ID", "INS_ID", "REPORT_DATA", "ITEM_TYPE"), // 字段必须是大写
            Some(s"${id}"), //Some(s"ID like \'${biaoshi}${id}\'"),
            Some(Configs.getString(Constants.ZOOKEEPER_SERVER))
          )
          phoenixDF.printSchema()
          ss.sqlContext.udf.register("middleValue", new MiddleValueUDAF())
          ss.sqlContext.udf.register("AMean", new AlgorithmAMeanValueUDAF())
          ss.sqlContext.udf.register("ASD", new AlgorithmASDValueUDAF())
          ss.sqlContext.udf.register("MCV", new AlgorithmMedSIRCVValueUDAF())
          ss.sqlContext.udf.register("MmiddleValue", new AlgorithmMedSIRMiddleValueUDAF())
          val dataDF = phoenixDF.filter("REPORT_DATA!=999d and ITEM_TYPE=1")
            .select("ID", "CENTER_ID", "PLAN_ID", "SPEC_ID", "ITEM_ID", "TIMES_ID", "BATCH_ID",
              "CAL_MATERIAL_BRANDS_ID", "METHOD_ID", "INS_BRANDS_ID", "LAB_INS_ID", "REAGENT_BRANDS_ID", "INS_ID", "REPORT_DATA") //.cache()

          dataDF.cache()

          /** ***********************原始数据不分组 ****************************/
          dataDF.createOrReplaceTempView("tp")
          //
          //          // 如果有nan值，保存到oracle报错 java.sql.BatchUpdateException: 内部错误: Overflow Exception trying to bind NaN
          val resultDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID) as cn" +
            ",middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) or isnull(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) end as cv" +
            ",max(REPORT_DATA) as ma,min(REPORT_DATA) as mi from tp group by CENTER_ID,PLAN_ID,SPEC_ID," +
            "ITEM_ID,TIMES_ID,BATCH_ID").cache()


          import ss.implicits._
          val resultDFTransfer = resultDF.rdd.map(value => {

            val pk_id: String = value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + noGroupNumber

            (value.getAs[Integer](0), value.getAs[Integer](1), value.getAs[Integer](2),
              value.getAs[Integer](3), value.getAs[Integer](4), value.getAs[Integer](5),
              value.getAs[Long](6), value.getAs[Double](7), value.getAs[Double](8),
              value.getAs[Double](9), value.getAs[Double](10), value.getAs[Double](11), value.getAs[Double](12),
              0, 0, pk_id, 0)
          }).toDF

          resultDF.createOrReplaceTempView("result")

          val totalDF = ss.sql("select tt.*,tp.REPORT_DATA from result tt join tp on " +
            "tp.CENTER_ID = tt.CENTER_ID and tp.PLAN_ID = tt.PLAN_ID and tp.SPEC_ID = tt.SPEC_ID and " +
            "tp.ITEM_ID = tt.ITEM_ID and tp.TIMES_ID = tt.TIMES_ID " +
            "and tp.BATCH_ID = tt.BATCH_ID")

          totalDF.createOrReplaceTempView("totalTemp")

          val p3sdDfTemp = ss.sql("select * from totalTemp where REPORT_DATA >= (mn-3*sd) and REPORT_DATA <= (mn+3*sd)")

          p3sdDfTemp.createOrReplaceTempView("p3Total")

          val p3sdDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID),middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) or isnull(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) " +
            "end as cv,max(REPORT_DATA),min(REPORT_DATA) from p3Total group by CENTER_ID," +
            "PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID")


          val p3sdDFTransfer = p3sdDF.rdd.map(value => {

            val pk_id: String = value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + noGroupNumberP3sd

            (value.getAs[Integer](0), value.getAs[Integer](1), value.getAs[Integer](2),
              value.getAs[Integer](3), value.getAs[Integer](4), value.getAs[Integer](5),
              value.getAs[Long](6), value.getAs[Double](7), value.getAs[Double](8),
              value.getAs[Double](9), value.getAs[Double](10), value.getAs[Double](11), value.getAs[Double](12),
              0, 0, pk_id, 3)
          }).toDF

          val p2sdDfTemp = ss.sql("select * from totalTemp where REPORT_DATA >= (mn-2*sd) and REPORT_DATA <= (mn+2*sd)")

          p2sdDfTemp.createOrReplaceTempView("p2Total")



          val p2sdDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID),middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) or isnull(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) " +
            "end as cv,max(REPORT_DATA),min(REPORT_DATA) from p2Total group by CENTER_ID," +
            "PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID")



          val p2sdDFTransfer = p2sdDF.rdd.map(value => {

            val pk_id: String = value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + noGroupNumberP2sd

            (value.getAs[Integer](0), value.getAs[Integer](1), value.getAs[Integer](2),
              value.getAs[Integer](3), value.getAs[Integer](4), value.getAs[Integer](5),
              value.getAs[Long](6), value.getAs[Double](7), value.getAs[Double](8),
              value.getAs[Double](9), value.getAs[Double](10), value.getAs[Double](11), value.getAs[Double](12),
              0, 0, pk_id, 2)
          }).toDF

          /** ************************校准物厂商 ***************************/

          val resultCALDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID)" +
            ",middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) or isnull(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) end as cv," +
            "max(REPORT_DATA),min(REPORT_DATA),CAL_MATERIAL_BRANDS_ID from tp group by CENTER_ID,PLAN_ID,SPEC_ID," +
            "ITEM_ID,TIMES_ID,BATCH_ID,CAL_MATERIAL_BRANDS_ID")

          val resultCALDFTransfer = resultCALDF.rdd.map(value => {

            val pk_id: String = value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + value.getAs[Integer](13) + calBrandsId

            (value.getAs[Integer](0), value.getAs[Integer](1), value.getAs[Integer](2),
              value.getAs[Integer](3), value.getAs[Integer](4), value.getAs[Integer](5),
              value.getAs[Long](6), value.getAs[Double](7), value.getAs[Double](8),
              value.getAs[Double](9), value.getAs[Double](10), value.getAs[Double](11), value.getAs[Double](12),
              1, value.getAs[Integer](13), pk_id, 0)
          }).toDF


          resultCALDF.createOrReplaceTempView("resultCAL")


          val totalCALDF = ss.sql("select tt.*,tp.REPORT_DATA from resultCAL tt join tp on " +
            "tp.CENTER_ID = tt.CENTER_ID and tp.PLAN_ID = tt.PLAN_ID and tp.SPEC_ID = tt.SPEC_ID and " +
            "tp.ITEM_ID = tt.ITEM_ID and tp.TIMES_ID = tt.TIMES_ID " +
            "and tp.BATCH_ID = tt.BATCH_ID and tp.CAL_MATERIAL_BRANDS_ID=tt.CAL_MATERIAL_BRANDS_ID")


          totalCALDF.createOrReplaceTempView("totalCALTemp")

          val totalCALp3Temp = ss.sql("select * from totalCALTemp where REPORT_DATA >=(mn - 3*sd) and REPORT_DATA <= (mn + 3*sd)")

          totalCALp3Temp.createOrReplaceTempView("p3totalCAL")

          val p3sdCALDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID),middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) or isnull(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) " +
            "end as cv,max(REPORT_DATA),min(REPORT_DATA),CAL_MATERIAL_BRANDS_ID from p3totalCAL group by CENTER_ID," +
            "PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,CAL_MATERIAL_BRANDS_ID")


          val p3sdCALDFTransfer = p3sdCALDF.rdd.map(value => {

            val pk_id: String = value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + value.getAs[Integer](13) + calBrandsIdP3sd

            (value.getAs[Integer](0), value.getAs[Integer](1), value.getAs[Integer](2),
              value.getAs[Integer](3), value.getAs[Integer](4), value.getAs[Integer](5),
              value.getAs[Long](6), value.getAs[Double](7), value.getAs[Double](8),
              value.getAs[Double](9), value.getAs[Double](10), value.getAs[Double](11), value.getAs[Double](12),
              1, value.getAs[Integer](13), pk_id, 3)
          }).toDF

          val totalCALp2Temp = ss.sql("select * from totalCALTemp where REPORT_DATA >=(mn - 2*sd) and REPORT_DATA <= (mn + 2*sd)")

          totalCALp2Temp.createOrReplaceTempView("p2totalCAL")


          val p2sdCALDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID),middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) or isnull(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) " +
            "end as cv,max(REPORT_DATA),min(REPORT_DATA),CAL_MATERIAL_BRANDS_ID from p2totalCAL group by CENTER_ID," +
            "PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,CAL_MATERIAL_BRANDS_ID")

          val p2sdCALDFTransfer = p2sdCALDF.rdd.map(value => {

            val pk_id: String = value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + value.getAs[Integer](13) + calBrandsIdP2sd

            (value.getAs[Integer](0), value.getAs[Integer](1), value.getAs[Integer](2),
              value.getAs[Integer](3), value.getAs[Integer](4), value.getAs[Integer](5),
              value.getAs[Long](6), value.getAs[Double](7), value.getAs[Double](8),
              value.getAs[Double](9), value.getAs[Double](10), value.getAs[Double](11), value.getAs[Double](12),
              1, value.getAs[Integer](13), pk_id, 2)
          }).toDF



          /** ************************仪器厂商  INS_BRANDS_ID ***************************/


          val resultLabDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID)" +
            ",middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) or isnull(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) end as cv,max(REPORT_DATA),min(REPORT_DATA),INS_BRANDS_ID from tp group by CENTER_ID,PLAN_ID,SPEC_ID," +
            "ITEM_ID,TIMES_ID,BATCH_ID,INS_BRANDS_ID")

          val resultLabDFTransfer = resultLabDF.rdd.map(value => {

            val pk_id: String = value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + value.getAs[Integer](13) + labId

            (value.getAs[Integer](0), value.getAs[Integer](1), value.getAs[Integer](2),
              value.getAs[Integer](3), value.getAs[Integer](4), value.getAs[Integer](5),
              value.getAs[Long](6), value.getAs[Double](7), value.getAs[Double](8),
              value.getAs[Double](9), value.getAs[Double](10), value.getAs[Double](11), value.getAs[Double](12),
              2, value.getAs[Integer](13), pk_id, 0)
          }).toDF


          resultLabDF.createOrReplaceTempView("resultLab")


          val totalLabDF = ss.sql("select tt.*,tp.REPORT_DATA from resultLab tt join tp on " +
            "tp.CENTER_ID = tt.CENTER_ID and tp.PLAN_ID = tt.PLAN_ID and tp.SPEC_ID = tt.SPEC_ID and " +
            "tp.ITEM_ID = tt.ITEM_ID and tp.TIMES_ID = tt.TIMES_ID " +
            "and tp.BATCH_ID = tt.BATCH_ID and tp.INS_BRANDS_ID=tt.INS_BRANDS_ID")


          totalLabDF.createOrReplaceTempView("totalLabTemp")


          val totalLabp3Temp = ss.sql("select * from totalLabTemp where REPORT_DATA >=(mn - 3*sd) and REPORT_DATA <=(mn + 3*sd)")

          totalLabp3Temp.createOrReplaceTempView("p3totalLab")



          val p3sdLabDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID),middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) or isnull(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) " +
            "end as cv,max(REPORT_DATA),min(REPORT_DATA),INS_BRANDS_ID from p3totalLab group by CENTER_ID," +
            "PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,INS_BRANDS_ID")

          val p3sdLabDFTransfer = p3sdLabDF.rdd.map(value => {

            val pk_id: String = value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + value.getAs[Integer](13) + labIdP3sd

            (value.getAs[Integer](0), value.getAs[Integer](1), value.getAs[Integer](2),
              value.getAs[Integer](3), value.getAs[Integer](4), value.getAs[Integer](5),
              value.getAs[Long](6), value.getAs[Double](7), value.getAs[Double](8),
              value.getAs[Double](9), value.getAs[Double](10), value.getAs[Double](11), value.getAs[Double](12),
              2, value.getAs[Integer](13), pk_id, 3)
          }).toDF

          val totalLabp2Temp = ss.sql("select * from totalLabTemp where REPORT_DATA >= (mn - 2*sd) and REPORT_DATA <= (mn + 2*sd)")

          totalLabp2Temp.createOrReplaceTempView("p2totalLab")

          val p2sdLabDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID),middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) or isnull(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) " +
            "end as cv,max(REPORT_DATA),min(REPORT_DATA),INS_BRANDS_ID from p2totalLab group by CENTER_ID," +
            "PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,INS_BRANDS_ID")

          val p2sdLabDFTransfer = p2sdLabDF.rdd.map(value => {

            val pk_id: String = value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + value.getAs[Integer](13) + labIdP2sd

            (value.getAs[Integer](0), value.getAs[Integer](1), value.getAs[Integer](2),
              value.getAs[Integer](3), value.getAs[Integer](4), value.getAs[Integer](5),
              value.getAs[Long](6), value.getAs[Double](7), value.getAs[Double](8),
              value.getAs[Double](9), value.getAs[Double](10), value.getAs[Double](11), value.getAs[Double](12),
              2, value.getAs[Integer](13), pk_id, 2)
          }).toDF



          /** ***************************方法 ************************/

          val resultMethodDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID)" +
            ",middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) or isnull(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) end as cv,max(REPORT_DATA),min(REPORT_DATA),METHOD_ID from tp group by CENTER_ID,PLAN_ID,SPEC_ID," +
            "ITEM_ID,TIMES_ID,BATCH_ID,METHOD_ID")

          val resultMethodDFTransfer = resultMethodDF.rdd.map(value => {

            val pk_id: String = value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + value.getAs[Integer](13) + methodId

            (value.getAs[Integer](0), value.getAs[Integer](1), value.getAs[Integer](2),
              value.getAs[Integer](3), value.getAs[Integer](4), value.getAs[Integer](5),
              value.getAs[Long](6), value.getAs[Double](7), value.getAs[Double](8),
              value.getAs[Double](9), value.getAs[Double](10), value.getAs[Double](11), value.getAs[Double](12),
              4, value.getAs[Integer](13), pk_id, 0)
          }).toDF


          resultMethodDF.createOrReplaceTempView("resultMethod")


          val totalMethodDF = ss.sql("select tt.*,tp.REPORT_DATA from resultMethod tt join tp on " +
            "tp.CENTER_ID = tt.CENTER_ID and tp.PLAN_ID = tt.PLAN_ID and tp.SPEC_ID = tt.SPEC_ID and " +
            "tp.ITEM_ID = tt.ITEM_ID and tp.TIMES_ID = tt.TIMES_ID " +
            "and tp.BATCH_ID = tt.BATCH_ID and tp.METHOD_ID=tt.METHOD_ID")


          totalMethodDF.createOrReplaceTempView("totalMethodTemp")

          val totalMethodp3Temp = ss.sql("select * from totalMethodTemp where REPORT_DATA >=(mn - 3*sd) and REPORT_DATA <= (mn + 3*sd)")

          totalMethodp3Temp.createOrReplaceTempView("p3totalMethod")


          val p3sdMethodDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID),middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) or isnull(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) " +
            "end as cv,max(REPORT_DATA),min(REPORT_DATA),METHOD_ID from p3totalMethod group by CENTER_ID," +
            "PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,METHOD_ID")


          val p3sdMethodDFTransfer = p3sdMethodDF.rdd.map(value => {

            val pk_id: String = value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + value.getAs[Integer](13) + methodIdP3sd

            (value.getAs[Integer](0), value.getAs[Integer](1), value.getAs[Integer](2),
              value.getAs[Integer](3), value.getAs[Integer](4), value.getAs[Integer](5),
              value.getAs[Long](6), value.getAs[Double](7), value.getAs[Double](8),
              value.getAs[Double](9), value.getAs[Double](10), value.getAs[Double](11), value.getAs[Double](12),
              4, value.getAs[Integer](13), pk_id, 3)
          }).toDF

          val totalMethodp2Temp = ss.sql("select * from totalMethodTemp where REPORT_DATA >=(mn - 2*sd) and REPORT_DATA <= (mn + 2*sd)")

          totalMethodp2Temp.createOrReplaceTempView("p2totalMethod")


          val p2sdMethodDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID),middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) or isnull(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) " +
            "end as cv,max(REPORT_DATA),min(REPORT_DATA),METHOD_ID from p2totalMethod group by CENTER_ID," +
            "PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,METHOD_ID")

          val p2sdMethodDFTransfer = p2sdMethodDF.rdd.map(value => {

            val pk_id: String = value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + value.getAs[Integer](13) + methodIdP2sd

            (value.getAs[Integer](0), value.getAs[Integer](1), value.getAs[Integer](2),
              value.getAs[Integer](3), value.getAs[Integer](4), value.getAs[Integer](5),
              value.getAs[Long](6), value.getAs[Double](7), value.getAs[Double](8),
              value.getAs[Double](9), value.getAs[Double](10), value.getAs[Double](11), value.getAs[Double](12),
              4, value.getAs[Integer](13), pk_id, 2)
          }).toDF



          /** ***************************试剂厂商  REAGENT_BRANDS_ID ************************/


          val resultReagentDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID)" +
            ",middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) or isnull(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) end as cv,max(REPORT_DATA),min(REPORT_DATA),REAGENT_BRANDS_ID from tp group by CENTER_ID,PLAN_ID,SPEC_ID," +
            "ITEM_ID,TIMES_ID,BATCH_ID,REAGENT_BRANDS_ID")


          val resultReagentDFTransfer = resultReagentDF.rdd.map(value => {

            val pk_id: String = value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + value.getAs[Integer](13) + reagentId

            (value.getAs[Integer](0), value.getAs[Integer](1), value.getAs[Integer](2),
              value.getAs[Integer](3), value.getAs[Integer](4), value.getAs[Integer](5),
              value.getAs[Long](6), value.getAs[Double](7), value.getAs[Double](8),
              value.getAs[Double](9), value.getAs[Double](10), value.getAs[Double](11), value.getAs[Double](12),
              5, value.getAs[Integer](13), pk_id, 0)
          }).toDF



          resultReagentDF.createOrReplaceTempView("resultReagent")


          val totalReagentDF = ss.sql("select tt.*,tp.REPORT_DATA from resultReagent tt join tp on " +
            "tp.CENTER_ID = tt.CENTER_ID and tp.PLAN_ID = tt.PLAN_ID and tp.SPEC_ID = tt.SPEC_ID and " +
            "tp.ITEM_ID = tt.ITEM_ID and tp.TIMES_ID = tt.TIMES_ID " +
            "and tp.BATCH_ID = tt.BATCH_ID and tp.REAGENT_BRANDS_ID=tt.REAGENT_BRANDS_ID")



          totalReagentDF.createOrReplaceTempView("totalReagentTemp")


          val totalReagentp3Temp = ss.sql("select * from totalReagentTemp where REPORT_DATA >=(mn - 3*sd) and REPORT_DATA <= (mn + 3*sd)")

          totalReagentp3Temp.createOrReplaceTempView("p3totalReagent")

          val p3sdReagentDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID),middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) or isnull(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) " +
            "end as cv,max(REPORT_DATA),min(REPORT_DATA),REAGENT_BRANDS_ID from p3totalReagent group by CENTER_ID," +
            "PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,REAGENT_BRANDS_ID")

          val p3sdReagentDFTransfer = p3sdReagentDF.rdd.map(value => {

            val pk_id: String = value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + value.getAs[Integer](13) + reagentIdP3sd

            (value.getAs[Integer](0), value.getAs[Integer](1), value.getAs[Integer](2),
              value.getAs[Integer](3), value.getAs[Integer](4), value.getAs[Integer](5),
              value.getAs[Long](6), value.getAs[Double](7), value.getAs[Double](8),
              value.getAs[Double](9), value.getAs[Double](10), value.getAs[Double](11), value.getAs[Double](12),
              5, value.getAs[Integer](13), pk_id, 3)
          }).toDF


          val totalReagentp2Temp = ss.sql("select * from totalReagentTemp where REPORT_DATA >=(mn - 2*sd) and REPORT_DATA <= (mn + 2*sd)")

          totalReagentp2Temp.createOrReplaceTempView("p2totalReagent")


          val p2sdReagentDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID),middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) or isnull(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) " +
            "end as cv,max(REPORT_DATA),min(REPORT_DATA),REAGENT_BRANDS_ID from p2totalReagent group by CENTER_ID," +
            "PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,REAGENT_BRANDS_ID")

          val p2sdReagentDFTransfer = p2sdReagentDF.rdd.map(value => {

            val pk_id: String = value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + value.getAs[Integer](13) + reagentIdP2sd

            (value.getAs[Integer](0), value.getAs[Integer](1), value.getAs[Integer](2),
              value.getAs[Integer](3), value.getAs[Integer](4), value.getAs[Integer](5),
              value.getAs[Long](6), value.getAs[Double](7), value.getAs[Double](8),
              value.getAs[Double](9), value.getAs[Double](10), value.getAs[Double](11), value.getAs[Double](12),
              5, value.getAs[Integer](13), pk_id, 2)
          }).toDF


          /** 仪器型号  INS_ID **/


          val resultINSDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID)" +
            ",middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) or isnull(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) end as cv,max(REPORT_DATA),min(REPORT_DATA),INS_ID from tp group by CENTER_ID,PLAN_ID,SPEC_ID," +
            "ITEM_ID,TIMES_ID,BATCH_ID,INS_ID")

          val resultINSDFTransfer = resultINSDF.rdd.map(value => {

            val pk_id: String = value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + value.getAs[Integer](13) + insId

            (value.getAs[Integer](0), value.getAs[Integer](1), value.getAs[Integer](2),
              value.getAs[Integer](3), value.getAs[Integer](4), value.getAs[Integer](5),
              value.getAs[Long](6), value.getAs[Double](7), value.getAs[Double](8),
              value.getAs[Double](9), value.getAs[Double](10), value.getAs[Double](11), value.getAs[Double](12),
              3, value.getAs[Integer](13), pk_id, 0)
          }).toDF



          resultINSDF.createOrReplaceTempView("resultIns")


          val totalINSDF = ss.sql("select tt.*,tp.REPORT_DATA from resultIns tt join tp on " +
            "tp.CENTER_ID = tt.CENTER_ID and tp.PLAN_ID = tt.PLAN_ID and tp.SPEC_ID = tt.SPEC_ID and " +
            "tp.ITEM_ID = tt.ITEM_ID and tp.TIMES_ID = tt.TIMES_ID " +
            "and tp.BATCH_ID = tt.BATCH_ID and tp.INS_ID=tt.INS_ID")



          totalINSDF.createOrReplaceTempView("totalInsTemp")


          val totalInstp3Temp = ss.sql("select * from totalInsTemp where REPORT_DATA >=(mn - 3*sd) and REPORT_DATA <= (mn + 3*sd)")

          totalInstp3Temp.createOrReplaceTempView("p3totalIns")

          val p3sdINSDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID),middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) or isnull(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) " +
            "end as cv,max(REPORT_DATA),min(REPORT_DATA),INS_ID from p3totalIns group by CENTER_ID," +
            "PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,INS_ID")

          val p3sdINSDFTransfer = p3sdINSDF.rdd.map(value => {

            val pk_id: String = value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + value.getAs[Integer](13) + insIdP3sd

            (value.getAs[Integer](0), value.getAs[Integer](1), value.getAs[Integer](2),
              value.getAs[Integer](3), value.getAs[Integer](4), value.getAs[Integer](5),
              value.getAs[Long](6), value.getAs[Double](7), value.getAs[Double](8),
              value.getAs[Double](9), value.getAs[Double](10), value.getAs[Double](11), value.getAs[Double](12),
              3, value.getAs[Integer](13), pk_id, 3)
          }).toDF


          val totalInstp2Temp = ss.sql("select * from totalInsTemp where REPORT_DATA >=(mn - 2*sd) and REPORT_DATA <= (mn + 2*sd)")

          totalInstp2Temp.createOrReplaceTempView("p2totalIns")


          val p2sdINSDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID),middleValue(REPORT_DATA) as middle,mean(REPORT_DATA) as mn," +
            "case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) or isnull(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) " +
            "end as cv,max(REPORT_DATA),min(REPORT_DATA),INS_ID from p2totalIns group by CENTER_ID," +
            "PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,INS_ID")

          val p2sdINSDFTransfer = p2sdINSDF.rdd.map(value => {

            val pk_id: String = value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + value.getAs[Integer](13) + insIdP2sd

            (value.getAs[Integer](0), value.getAs[Integer](1), value.getAs[Integer](2),
              value.getAs[Integer](3), value.getAs[Integer](4), value.getAs[Integer](5),
              value.getAs[Long](6), value.getAs[Double](7), value.getAs[Double](8),
              value.getAs[Double](9), value.getAs[Double](10), value.getAs[Double](11), value.getAs[Double](12),
              3, value.getAs[Integer](13), pk_id, 2)
          }).toDF


          // 稳健统计
          val wenDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,count(BATCH_ID),MmiddleValue(REPORT_DATA) as middle,AMean(REPORT_DATA) as mn," +
            "case when isnan(ASD(REPORT_DATA)) then 0 else ASD(REPORT_DATA) end as sd," +
            "case when isnan(MCV(REPORT_DATA)) then 0 else MCV(REPORT_DATA) end as cv," +
            "max(REPORT_DATA) as ma,min(REPORT_DATA) as mi from tp group by CENTER_ID,PLAN_ID,SPEC_ID," +
            "ITEM_ID,TIMES_ID,BATCH_ID")



          val wenDFTransferNotNull = wenDF.rdd.filter(value => {
            value.getAs[Object](7) != null

          }).map(value => {

            val pk_id: String = value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + noGroupNumberWen

            (value.getAs[Integer](0), value.getAs[Integer](1), value.getAs[Integer](2),
              value.getAs[Integer](3), value.getAs[Integer](4), value.getAs[Integer](5),
              value.getAs[Long](6), value.getAs[Double](7), value.getAs[Double](8),
              value.getAs[Double](9), value.getAs[Double](10), value.getAs[Double](11), value.getAs[Double](12),
              6, 2, pk_id, 0)


          }).toDF




          val wenDFTransferNull = wenDF.rdd.filter(value => {
            value.getAs[Object](7) == null
          }).map(value => {

            val pk_id: String = value.getAs[Integer](0) + "" +
              value.getAs[Integer](1) + "" + value.getAs[Integer](2) + "" +
              value.getAs[Integer](4) + "" + value.getAs[Integer](3) +
              value.getAs[Integer](5) + noGroupNumberWen


            (value.getAs[Integer](0), value.getAs[Integer](1), value.getAs[Integer](2),
              value.getAs[Integer](3), value.getAs[Integer](4), value.getAs[Integer](5),
              value.getAs[Long](6), 99999, value.getAs[Double](8),
              value.getAs[Double](9), 99999, value.getAs[Double](11), value.getAs[Double](12),
              6, 2, pk_id, 0)


          }).toDF


          val allDF = wenDFTransferNotNull.union(wenDFTransferNull).union(resultINSDFTransfer).union(p3sdINSDFTransfer).union(p2sdINSDFTransfer)
            .union(resultDFTransfer).union(p3sdDFTransfer).union(p2sdDFTransfer)
            .union(resultCALDFTransfer).union(p3sdCALDFTransfer).union(p2sdCALDFTransfer)
            .union(resultMethodDFTransfer).union(p3sdMethodDFTransfer).union(p2sdMethodDFTransfer)
            .union(resultLabDFTransfer).union(p3sdLabDFTransfer).union(p2sdLabDFTransfer)
            .union(resultReagentDFTransfer).union(p3sdReagentDFTransfer).union(p2sdReagentDFTransfer)


          allDF.repartition(8).foreachPartition(rows => {


            val connect: Connection = C3p0Utils.getConnect()


            val pstmt = connect.prepareStatement(OracleUtilsOptimize.sql)

            connect.setAutoCommit(false)
            rows.foreach(value => {

              OracleUtilsOptimize.insertBatch(pstmt, value)


            })


            pstmt.executeBatch()

            connect.commit()
            C3p0Utils.release(connect, pstmt)


          })

          dataDF.unpersist()

        }
        accu.reset()
        myAcc.reset()
        kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRangs)


      }


    })


    ssc.start()
    ssc.awaitTermination()


  }
}
