package com.bioqas.product

import java.sql.Connection

import com.bioqas.C3p0Utils
import com.bioqas.test.{Configs, MiddleValueUDAF, OracleDialect}
import com.bioqas.utils._
import org.apache.commons.codec.digest.DigestUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.phoenix.spark._
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer


// phoenixTableAsDataFrame
import scala.collection.mutable

/**
  * Created by chailei on 18/7/12.
  */
object OracleToHbaseComputeGroupPP {

  var batchWj: Array[Int] = _

  var batchLab: Array[Int] = _

  var batchPercent: Array[Int] = _

  def main(args: Array[String]) {

    val conf = new SparkConf()
    //              .setAppName("TestApp")
    //              .setMaster("local[9]")
    //              .set("spark.streaming.kafka.maxRatePerPartition", "15")
    //              .set("spark.sql.shuffle.partitions", "10")
    //      "metadata.broker.list" -> "192.168.8.155:9092,192.168.8.156:9092,192.168.8.157:9092"

    // metadata.broker.list 参数为什么不能用ip:端口
    val paramMap = Map[String, Object](
      "bootstrap.servers" -> Configs.getString(Constants.KAFKA_SERVER),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group_group", // 原来是test  ------ 测试group test_group_progress
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "max.partition.fetch.bytes" -> (2621440: java.lang.Integer), //default: 1048576
      "request.timeout.ms" -> (90000: java.lang.Integer), //default: 60000
      "session.timeout.ms" -> (60000: java.lang.Integer) //default: 30000
    )
    //"metadata.broker.list" -> "mini2:9092,mini3:9092,mini4:9092"
    val topics = "group"
    val topicSet = topics.split(",")
    val ssc = new StreamingContext(conf, Seconds(Configs.getInt(Constants.STREAMING_GROUP_DATA)))

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
    ssc.sparkContext.register(myAcc, "myAcc")
    ssc.sparkContext.register(idAcc, "idAcc")
    val ids = mutable.HashSet[String]()
    val idBroadCast = ssc.sparkContext.broadcast(ids)
    JdbcDialects.registerDialect(OracleDialect)
    kafkaStream.foreachRDD(rdd => {

      if (rdd != null) {

        val offsetRangs = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        idBroadCast.value.clear()

        rdd.foreachPartition(partitions => {


          if (partitions != null) {


            partitions.foreach(line => {

              val key = JsonParseGroupUtil.parseJson(line.value())

              if (!"".equals(key)) {
                idAcc.add(key)
              }

            })

          }
        })

        if (!"".equals(idAcc.value)) {

          println("id " + idAcc.value)
          val ss = SparkSession.builder().config(conf).getOrCreate()
          val groupId = mutable.StringBuilder.newBuilder
          val id = mutable.StringBuilder.newBuilder
          val ids: Array[String] = idAcc.value.split(",")
          println("idAcc = " + idAcc.value)
          val evaluate = mutable.HashMap[String, String]()

          for (x: String <- ids.toSet) {
            println(x)
            val split: Array[String] = x.split("-")
            println(split(0) + " " + split(1))
            val idSplit: Array[String] = split(0).split("_")
            val key = idSplit(1) + "_" + idSplit(2)

            id.append("id like ").append("\'").append("%").append(key).append("%").append("\'").append(" or ")
            groupId.append("group_id=").append(idSplit(0))
              .append(" and ").append("times_id=").append(idSplit(1))
              .append(" and ").append("item_id=").append(idSplit(2)).append(" or ")

            val evaluateArray: Array[String] = split(1).split("_")

            evaluate.put(evaluateArray(0), evaluateArray(1) + "," + evaluateArray(2))

          }
          val evaluateBroadcast: Broadcast[mutable.HashMap[String, String]] = ss.sparkContext.broadcast(evaluate)
          groupId.delete(groupId.length - 4, groupId.length - 1)
          id.delete(id.length - 4, id.length - 1)
          println("groupid = " + groupId)
          println("id = " + id)
          val tUserDF = ss.read.format("jdbc").options(Map(
            "url" -> Configs.getString(Constants.DB_URL),
            "user" -> Configs.getString(Constants.DB_USER),
            "password" -> Configs.getString(Constants.DB_PASSWORD),
            "dbtable" -> s"(select * from ${Configs.getString(Constants.DB_USER)}.TF_E_HPV_ITEM_GROUP_LAB where ${groupId}) t",
            "driver" -> Configs.getString(Constants.DB_DRIVER)
          )).load()

          //          val tUserDF = ss.read.format("jdbc").options(Map(
          //            "url" -> "jdbc:oracle:thin:@47.92.136.8:1521:biodev",
          //            "user" -> "biodev",
          //            "password" -> "Bio#2018#Ora",
          //            "dbtable" -> s"(select * from BIODEV.TF_E_HPV_ITEM_GROUP_LAB where ${groupId}) t",
          //            "driver" -> "oracle.jdbc.driver.OracleDriver"
          //          )).load()

          println("TF_E_HPV_ITEM_GROUP_LAB")
          //          tUserDF.show()
          tUserDF.createOrReplaceTempView("groupData")
          //          tUserDF.map( value => {
          //
          //          })


          //        println("accu = " + accu.value)
          //
          //        for(x:String <- myAcc.value.split(",").toSet){// 这里不加String类型会报错
          //          val split: Array[String] = x.split("_")
          //
          //          val key = split(0) + "%" + split(2) + split(3)
          //          id.append("id like ").append("\'").append("%").append(key).append("%").append("\'").append(" or ")
          //        }
          //        id.delete(id.length-4,id.length-1)
          println("id " + id.toString())
          println("myAcc " + myAcc.value + "   " + myAcc.value.equals(""))
          println("broadcast = " + idBroadCast.value.size)
          //if(idBroadCast.value.size != 0){
          //        if(!myAcc.value.equals("")){// 当没有数据的时候，不读取数据进行计算
          val biaoshi = "%"
          ss.sqlContext.udf.register("middleValue", new MiddleValueUDAF())
          val phoenixDF = ss.sqlContext.phoenixTableAsDataFrame(
            "TP_E_BD_REPORT_DATA_BUCKET", //TP_E_BD_REPORT_DATE_BUCKET
            Seq("ID", "CENTER_ID", "PLAN_ID", "SPEC_ID", "ITEM_ID", "BATCH_ID", "TIMES_ID",
              "LAB_ID", "REPORT_DATA", "LAB_NAME", "LAB_SPELL", "ITEM_PRECISION", "BATCH_NAME", "ITEM_TYPE"), // 字段必须是大写
            Some(s"${id}"), //Some(s"ID like \'${biaoshi}${id}\'"),
            Some(Configs.getString(Constants.ZOOKEEPER_SERVER))
          ) //.cache()
          phoenixDF.printSchema()
          ss.sqlContext.udf.register("middleValue", new MiddleValueUDAF())
          ss.sqlContext.udf.register("AMean", new AlgorithmAMeanValueUDAF())
          ss.sqlContext.udf.register("ASD", new AlgorithmASDValueUDAF())
          ss.sqlContext.udf.register("MCV", new AlgorithmMedSIRCVValueUDAF())
          ss.sqlContext.udf.register("MmiddleValue", new AlgorithmMedSIRMiddleValueUDAF())
          val dataDF = phoenixDF.filter("REPORT_DATA!=999d and ITEM_TYPE=1")
            .select("CENTER_ID", "PLAN_ID", "SPEC_ID", "ITEM_ID", "TIMES_ID", "BATCH_ID",
              "LAB_ID", "REPORT_DATA", "LAB_NAME", "LAB_SPELL", "ITEM_PRECISION", "BATCH_NAME")
          //            .cache()

          //          println("TP_E_BD_REPORT_DATA_BUCKET")
          //          dataDF.show()
          dataDF.createOrReplaceTempView("originData")

          val totalDF = ss.sql("select o.*,g.GROUP_ID,g.GROUP_NAME from originData o join groupData g on " +
            "o.LAB_ID = g.LAB_ID")

          totalDF.show()


          val progressDF: DataFrame = totalDF.select("CENTER_ID", "PLAN_ID", "SPEC_ID", "TIMES_ID", "ITEM_ID", "GROUP_ID")

          //          progressDF.cache()


          totalDF.createOrReplaceTempView("tp")


          val df = ss.sql("select tp.*,tt.mn,tt.sd,tt.middle from tp join (select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID," +
            "TIMES_ID,BATCH_ID,GROUP_ID,GROUP_NAME,BATCH_NAME,count(BATCH_ID),mean(REPORT_DATA) as mn,middleValue(REPORT_DATA) as middle,case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd " +
            "from tp group by CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,GROUP_ID,GROUP_NAME,BATCH_NAME) tt on " +
            "tp.CENTER_ID = tt.CENTER_ID and tp.PLAN_ID = tt.PLAN_ID and tp.SPEC_ID = tt.SPEC_ID and " +
            "tp.ITEM_ID = tt.ITEM_ID and tp.TIMES_ID = tt.TIMES_ID " +
            "and tp.BATCH_ID = tt.BATCH_ID and tp.GROUP_ID=tt.GROUP_ID and tp.GROUP_NAME=tt.GROUP_NAME") //.cache()

          //          df.show()


          df.createOrReplaceTempView("compute")

          val wj_p0 = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,TIMES_ID,ITEM_ID,BATCH_ID,BATCH_NAME,GROUP_ID,GROUP_NAME,count(BATCH_ID)," +
            "mean(REPORT_DATA) as mn,middleValue(REPORT_DATA) as middle,case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) or isnull(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) end as cv," +
            "max(REPORT_DATA) as ma,min(REPORT_DATA) as mi,case when isnan(AMean(REPORT_DATA)) then 0 else AMean(REPORT_DATA) end as amean," +
            "case when isnan(ASD(REPORT_DATA)) then 0 else ASD(REPORT_DATA) end as asd," +
            "case when isnan(MmiddleValue(REPORT_DATA)) then 0 else MmiddleValue(REPORT_DATA) end as mmiddle," +
            "case when isnan(MCV(REPORT_DATA)) then 0 else MCV(REPORT_DATA) end as mcv from compute " +
            "group by CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,GROUP_ID,GROUP_NAME,BATCH_NAME")


          val addColwjP0: UserDefinedFunction = ss.udf.register("addcolP0", (str: String) => "0")

          val wjP0Total = wj_p0.withColumn("DISCARD_NSD", addColwjP0(wj_p0("BATCH_NAME")))

          val p_3sd = ss.sql("select * from compute where REPORT_DATA <= (mn + 3*sd) and REPORT_DATA >= (mn - 3*sd)")

          p_3sd.createOrReplaceTempView("p_3sd")

          val wj_p3 = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,TIMES_ID,ITEM_ID,BATCH_ID,BATCH_NAME,GROUP_ID,GROUP_NAME,count(BATCH_ID)," +
            "mean(REPORT_DATA) as mn,middleValue(REPORT_DATA) as middle,case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) or isnull(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) end as cv," +
            "max(REPORT_DATA) as ma,min(REPORT_DATA) as mi,case when isnan(AMean(REPORT_DATA)) then 0 else AMean(REPORT_DATA) end as amean," +
            "case when isnan(ASD(REPORT_DATA)) then 0 else ASD(REPORT_DATA) end as asd," +
            "case when isnan(MmiddleValue(REPORT_DATA)) then 0 else MmiddleValue(REPORT_DATA) end as mmiddle," +
            "case when isnan(MCV(REPORT_DATA)) then 0 else MCV(REPORT_DATA) end as mcv from p_3sd " +
            "group by CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,GROUP_ID,GROUP_NAME,BATCH_NAME")

          val addColwjP3: UserDefinedFunction = ss.udf.register("addcolP3", (str: String) => "3")

          val wjP3Total = wj_p3.withColumn("DISCARD_NSD", addColwjP3(wj_p3("BATCH_NAME")))

          //          wjP3Total.show()

          val p_2sd = ss.sql("select * from compute where REPORT_DATA <= (mn + 2*sd) and REPORT_DATA >= (mn - 2*sd)")

          p_2sd.createOrReplaceTempView("p_2sd")

          val wj_p2 = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,TIMES_ID,ITEM_ID,BATCH_ID,BATCH_NAME,GROUP_ID,GROUP_NAME,count(BATCH_ID)," +
            "mean(REPORT_DATA) as mn,middleValue(REPORT_DATA) as middle,case when isnan(stddev(REPORT_DATA)) then 0 else stddev(REPORT_DATA) end as sd," +
            "case when isnan(stddev(REPORT_DATA)/mean(REPORT_DATA)) or isnull(stddev(REPORT_DATA)/mean(REPORT_DATA)) then 0 else stddev(REPORT_DATA)/mean(REPORT_DATA) end as cv," +
            "max(REPORT_DATA) as ma,min(REPORT_DATA) as mi,case when isnan(AMean(REPORT_DATA)) then 0 else AMean(REPORT_DATA) end as amean," +
            "case when isnan(ASD(REPORT_DATA)) then 0 else ASD(REPORT_DATA) end as asd," +
            "case when isnan(MmiddleValue(REPORT_DATA)) then 0 else MmiddleValue(REPORT_DATA) end as mmiddle," +
            "case when isnan(MCV(REPORT_DATA)) then 0 else MCV(REPORT_DATA) end as mcv from p_2sd " +
            "group by CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,GROUP_ID,GROUP_NAME,BATCH_NAME")

          val addColwjP2: UserDefinedFunction = ss.udf.register("addcolP2", (str: String) => "2")

          val wjP2Total = wj_p2.withColumn("DISCARD_NSD", addColwjP2(wj_p2("BATCH_NAME")))

          //          wjP2Total.show()

          val wjTotal2 = wjP0Total.union(wjP2Total).union(wjP3Total)



          //          wjTotal2.show()

          val addColwjP: UserDefinedFunction = ss.udf.register("addcolP", (str: String) => "0")

          val wjTotal1 = wj_p0.union(wj_p2).union(wj_p3)

          val wjTotal = wjTotal1.withColumn("DISCARD_NSD", addColwjP(wjTotal1("BATCH_NAME")))


          wjTotal.printSchema()


          val addProgressDFone: UserDefinedFunction = ss.udf.register("one", (str: String) => 1)

          progressDF.printSchema()



          val batchWjDF = progressDF.withColumn("PROGRESS", addProgressDFone(progressDF("CENTER_ID")))
            .repartition(1).distinct()


          //          println("count = " + wjTotal2.count())
          //          println("distinct count = " + wjTotal2.distinct().count())

          wjTotal2.repartition(3).foreachPartition(rows => {

            //            val connect: Connection = OracleWjDbUtils.getConnect()

            val connect: Connection = C3p0Utils.getConnect()

            val pstmt = connect.prepareStatement(OracleWjDbUtils.sql)


            connect.setAutoCommit(false)
            rows.foreach(value => {


              val pk_id = DigestUtils.md5Hex(value.getAs[Integer](3).toString + value.getAs[Integer](4).toString + value.getAs[Integer](5).toString +
                value.getAs[Integer](7).toString + value.getAs[String](20)).substring(0, 19)

              OracleWjDbUtils.insertBatch(pstmt, value, pk_id)


            })

            batchWj = pstmt.executeBatch()

            connect.commit()
            //            OracleWjDbUtils.close(connect, pstmt)
            C3p0Utils.release(connect, pstmt)


          })

          batchWjDF.repartition(1).foreachPartition(partition => {

            //            val connect: Connection = OracleGroupPPDbUtils.getConnect()

            val connect: Connection = C3p0Utils.getConnect()

            val pstmt = connect.prepareStatement(OracleGroupPPDbUtils.sql)

            connect.setAutoCommit(false)

            partition.foreach(value => {
              // "CENTER_ID","PLAN_ID","SPEC_ID","TIMES_ID","ITEM_ID","GROUP_ID"
              val pk_id = DigestUtils.md5Hex(value.getAs[Integer](3).toString + value.getAs[Integer](4).toString + value.getAs[Integer](5).toString +
                value.getAs[Integer](6).toString).substring(0, 10)

              //TODO groupid grouptype
              OracleGroupPPDbUtils.insertBatch(pstmt, value, pk_id)

            })

            batchPercent = pstmt.executeBatch()

            connect.commit()
            //            OracleGroupDbUtils.close(connect, pstmt)

            C3p0Utils.release(connect, pstmt)

          })



          //          val total = df.union(p_2sd).union(p_3sd)
          //          df.show()
          //          p_2sd.show()
          //          p_3sd.show()
          val total = p_2sd.union(p_3sd)


          println("total")
          total.printSchema()
          //          total.show()

          total.createOrReplaceTempView("total")

          val mmDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,GROUP_ID,count(BATCH_ID)" +
            ",max(mn),min(mn),max(middle),min(middle),GROUP_NAME,ITEM_PRECISION,BATCH_NAME " +
            "from total group by CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,TIMES_ID,BATCH_ID,GROUP_ID,GROUP_NAME,ITEM_PRECISION,BATCH_NAME")
          import ss.implicits._

          println("mmDF")
          //          mmDF.show()

          mmDF.printSchema()

          val mmRdd = mmDF.rdd.map(line => {
            val array = new ArrayBuffer[Double]()
            array.append(line.getAs[Double](8))
            array.append(line.getAs[Double](9))
            array.append(line.getAs[Double](10))
            array.append(line.getAs[Double](11))
            val max = array.max
            val min = array.min
            (line.getAs[Integer](0), line.getAs[Integer](1), line.getAs[Integer](2), line.getAs[Integer](3)
              , line.getAs[Integer](4), line.getAs[Integer](5), line.getAs[Integer](6), max.formatted("%.2f").toDouble, min.formatted("%.2f").toDouble, line.getAs[String](12), line.getAs[Double](13), line.getAs[String](14))
          }).toDF("CENTER_ID", "PLAN_ID", "SPEC_ID", "ITEM_ID", "TIMES_ID", "BATCH_ID", "GROUP_ID", "max", "min", "GROUP_NAME", "ITEM_PRECISION", "BATCH_NAME")

          mmRdd.printSchema()

          val targetRDD = mmRdd.rdd.map(line => {

            val precision = Math.pow(10, 0 - line.getAs[Double](10).toInt)
            println("precision = " + precision)
            ((line.getAs[Integer](0), line.getAs[Integer](1), line.getAs[Integer](2), line.getAs[Integer](3),
              line.getAs[Integer](4), line.getAs[Integer](5), line.getAs[Integer](6), line.getAs[String](9), line.getAs[String](11), line.getAs[Double](10).toInt),
              (line.getAs[Double](8) to line.getAs[Double](7) by precision))
          })
            .map(lines => {
              val sb = mutable.StringBuilder.newBuilder
              //                val arr = new  ArrayBuffer()
              val formatPrecision = lines._1._10
              for (value <- lines._2) {
                sb.append(lines._1._1).append(",")
                  .append(lines._1._2).append(",")
                  .append(lines._1._3).append(",")
                  .append(lines._1._4).append(",")
                  .append(lines._1._5).append(",")
                  .append(lines._1._6).append(",")
                  .append(lines._1._7).append(",")
                  .append(lines._1._8).append(",")
                  .append(lines._1._9).append(",")
                  .append(value.formatted(s"%.${formatPrecision}f")).append("\t")
                rddaccu.add(1)
              }

              sb.toString()
            }).flatMap(values => {
            values.split("\t")
          }) //.cache()

          val totalTargetDF = targetRDD.map(values => {
            val targetValues = values.split(",")
            (targetValues(0), targetValues(1), targetValues(2),
              targetValues(3), targetValues(4), targetValues(5), targetValues(6), targetValues(7), targetValues(8), targetValues(9))
          }).toDF("CENTER_ID", "PLAN_ID", "SPEC_ID", "ITEM_ID", "TIMES_ID", "BATCH_ID", "GROUP_ID", "GROUP_NAME", "BATCH_NAME", "target")

          totalTargetDF.printSchema()

          println("target")
          //          totalTargetDF.show()
          totalTargetDF.createOrReplaceTempView("target")


          val computeDF = ss.sql("select tp.*,target.target from tp join target on " +
            "tp.CENTER_ID = target.CENTER_ID and tp.PLAN_ID = target.PLAN_ID and tp.SPEC_ID = target.SPEC_ID and " +
            "tp.ITEM_ID = target.ITEM_ID and tp.TIMES_ID = target.TIMES_ID " +
            "and tp.BATCH_ID = target.BATCH_ID and tp.GROUP_ID = target.GROUP_ID and tp.GROUP_NAME=target.GROUP_NAME")

          println("compute")
          //          computeDF.show()


          println("computeDF")
          computeDF.printSchema()
          val scoreDF = computeDF.rdd.filter(line => {
            val value: mutable.HashMap[String, String] = evaluateBroadcast.value
            val evaluateValue: String = value.getOrElse(line.getAs[Integer](12) + line.getAs[Integer](4).toString + line.getAs[Integer](3).toString, "")
            println("evaluateValue " + evaluateValue)
            if (!"".equalsIgnoreCase(evaluateValue)) {
              val evalue: Array[String] = evaluateValue.split(",")
              if (evalue(0).toInt != 3) true else false

            } else {
              false
            }

          }).map(line => {
            val report_data = line.getAs[Double](7)
            val target = line.getAs[String](14).toDouble

            val value: mutable.HashMap[String, String] = evaluateBroadcast.value
            //            println(value.mkString(","))
            //            val evaluateValue: String = value.getOrElse("693" + line.getAs[Integer](4).toString + line.getAs[Integer](3).toString,"")
            val evaluateValue: String = value.getOrElse(line.getAs[Integer](12) + line.getAs[Integer](4).toString + line.getAs[Integer](3).toString, "")
            var minLimit = 0d
            var maxLimit = 0d
            var score = 0
            // println("evaluateValue " + evaluateValue)
            if (!"".equalsIgnoreCase(evaluateValue)) {
              val evalue: Array[String] = evaluateValue.split(",")
              // println(evaluate.toString())
              if (evalue(0).toInt == 1) {
                minLimit = target - evalue(1).toDouble
                maxLimit = target + evalue(1).toDouble

              } else if (evalue(0).toInt == 2) {
                minLimit = target - target * evalue(1).toDouble
                maxLimit = target + target * evalue(1).toDouble
              }

              if (report_data >= minLimit && report_data <= maxLimit) {
                score = 1
              }
            }

            (line.getAs[Integer](0), line.getAs[Integer](1), line.getAs[Integer](2), line.getAs[Integer](4),
              line.getAs[Integer](3), line.getAs[Integer](5), line.getAs[Integer](6), line.getAs[Double](7).formatted("%.2f"),
              line.getAs[String](8), line.getAs[String](9), line.getAs[String](11), line.getAs[Integer](12), line.getAs[String](13), line.getAs[String](14), score)
          }).toDF("CENTER_ID", "PLAN_ID", "SPEC_ID", "TIMES_ID", "ITEM_ID", "BATCH_ID", "LAB_ID", "REPORT_DATA", "LAB_NAME", "LAB_SPELL", "BATCH_NAME", "GROUP_ID", "GROUP_NAME", "target", "score")

          //          "CENTER_ID","PLAN_ID","SPEC_ID","ITEM_ID","TIMES_ID","BATCH_ID",
          //          "LAB_ID","REPORT_DATA","LAB_NAME","LAB_SPELL","ITEM_PRECISION","BATCH_NAME",g.GROUP_ID,g.GROUP_NAME,target,score

          scoreDF.printSchema()
          scoreDF.createOrReplaceTempView("scoreDF")

          val resultDF = ss.sql("select tt.CENTER_ID, tt.PLAN_ID,tt.SPEC_ID, tt.TIMES_ID,tt.ITEM_ID, " +
            "tt.BATCH_ID,tt.GROUP_ID,tt.GROUP_NAME,tt.BATCH_NAME,tt.target, (tt.num1/totalNum)*100 as per1,(tt.num2/totalNum)*100 as per2 from (select CENTER_ID, PLAN_ID,SPEC_ID, ITEM_ID, TIMES_ID, " +
            "BATCH_ID,GROUP_ID,GROUP_NAME,BATCH_NAME , target,count(1) as totalNum, count(if(score = 1,1,null)) as num1,count(if(score = 0,1,null)) as num2 " +
            "from scoreDF group by CENTER_ID, PLAN_ID,SPEC_ID, ITEM_ID, TIMES_ID,GROUP_ID, " +
            "BATCH_ID, target,GROUP_NAME,BATCH_NAME) tt")

          println("!!!!!!!!!!!!!!")

          scoreDF.printSchema()


          val addProgressDFtwo: UserDefinedFunction = ss.udf.register("two", (str: String) => 2)


          val batchLabDF = progressDF.withColumn("PROGRESS", addProgressDFtwo(progressDF("CENTER_ID")))
            .repartition(1).distinct()

          //          val broadcastLab: Broadcast[Array[Row]] = ss.sparkContext.broadcast(batchLabDF.collect())


          //          println("count = " + scoreDF.count())
          //          println("distinct count = " + scoreDF.distinct().count())

          //          scoreDF.show()
          //
          scoreDF.distinct().repartition(8).foreachPartition(rows => {


            //            val connect: Connection = OracleLabDbUtils.getConnect()
            val connect: Connection = C3p0Utils.getConnect()

            val pstmt = connect.prepareStatement(OracleLabDbUtils.sql)


            connect.setAutoCommit(false)
            rows.foreach(value => {

              //[22,302,57,1104,804,4385,97,128.60,临颍县陈庄乡医院检验科,C,180612,807, test-长春（4）,139.66999999999965,1]
              val pk_id = DigestUtils.md5Hex(value.getAs[Integer](3).toString + value.getAs[Integer](4).toString + value.getAs[Integer](5).toString +
                value.getAs[Integer](6).toString + value.getAs[String](7).toString + value.getAs[String](10).toString + value.getAs[String](11) + value.getAs[String](13).toString).substring(0, 19)


              OracleLabDbUtils.insertBatch(pstmt, value, pk_id)


            })

            batchLab = pstmt.executeBatch()
            connect.commit()
            //            OracleLabDbUtils.close(connect, pstmt)

            C3p0Utils.release(connect, pstmt)


          })

          batchLabDF.repartition(1).foreachPartition(partition => {

            //            val connect: Connection = OracleGroupPPDbUtils.getConnect()

            val connect: Connection = C3p0Utils.getConnect()

            val pstmt = connect.prepareStatement(OracleGroupPPDbUtils.sql)

            connect.setAutoCommit(false)

            partition.foreach(value => {
              // "CENTER_ID","PLAN_ID","SPEC_ID","TIMES_ID","ITEM_ID","GROUP_ID"
              val pk_id = DigestUtils.md5Hex(value.getAs[Integer](3).toString + value.getAs[Integer](4).toString + value.getAs[Integer](5).toString +
                value.getAs[Integer](6).toString).substring(0, 10)

              //TODO groupid grouptype
              OracleGroupPPDbUtils.insertBatch(pstmt, value, pk_id)

            })

            batchPercent = pstmt.executeBatch()

            connect.commit()
            //            OracleGroupDbUtils.close(connect, pstmt)
            C3p0Utils.release(connect, pstmt)

          })


          resultDF.printSchema()

          val addProgressDFthree: UserDefinedFunction = ss.udf.register("three", (str: String) => 3)


          val batchPercentDF = progressDF.withColumn("PROGRESS", addProgressDFthree(progressDF("CENTER_ID")))
            .repartition(1).distinct()

          //          val broadcastBatchPercent: Broadcast[Array[Row]] = ss.sparkContext.broadcast(batchPercentDF.collect())

          //          println("count = " + resultDF.count())
          //          println("distinct count = " + resultDF.distinct().count())

          //          resultDF.show()

          resultDF.repartition(3).foreachPartition(rows => {


            //            val connect: Connection = OracleGroupDbUtils.getConnect()

            val connect: Connection = C3p0Utils.getConnect()

            val pstmt = connect.prepareStatement(OracleGroupDbUtils.sql)


            connect.setAutoCommit(false)
            rows.foreach(value => {


              val pk_id = DigestUtils.md5Hex(value.getAs[Integer](3).toString + value.getAs[Integer](4).toString + value.getAs[Integer](5).toString +
                value.getAs[Integer](6).toString + value.getAs[String](9)).substring(0, 9)

              OracleGroupDbUtils.insertBatch(pstmt, value, pk_id)


            })

            batchPercent = pstmt.executeBatch()

            connect.commit()
            //            OracleGroupDbUtils.close(connect, pstmt)
            C3p0Utils.release(connect, pstmt)


          })

          batchPercentDF.repartition(1).foreachPartition(partition => {

            //            val connect: Connection = OracleGroupPPDbUtils.getConnect()

            val connect: Connection = C3p0Utils.getConnect()

            val pstmt = connect.prepareStatement(OracleGroupPPDbUtils.sql)

            connect.setAutoCommit(false)

            partition.foreach(value => {
              // "CENTER_ID","PLAN_ID","SPEC_ID","TIMES_ID","ITEM_ID","GROUP_ID"
              val pk_id = DigestUtils.md5Hex(value.getAs[Integer](3).toString + value.getAs[Integer](4).toString + value.getAs[Integer](5).toString +
                value.getAs[Integer](6).toString).substring(0, 10)

              //TODO groupid grouptype
              OracleGroupPPDbUtils.insertBatch(pstmt, value, pk_id)

            })

            batchPercent = pstmt.executeBatch()

            connect.commit()
            //            OracleGroupDbUtils.close(connect, pstmt)

            C3p0Utils.release(connect, pstmt)
          })


        }


        accu.reset()
        myAcc.reset()
        rddaccu.reset()
        kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRangs)

      }

    })




    ssc.start()
    ssc.awaitTermination()


  }

}
