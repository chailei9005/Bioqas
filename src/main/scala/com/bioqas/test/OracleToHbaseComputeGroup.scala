//package com.bioqas.test
//
//import com.bioqas.utils.{JsonParseGroupUtil, IdAccumulator, JSONParseUtil, PhoenixUtils}
//import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.phoenix.spark._
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.jdbc.JdbcDialects
//import org.apache.spark.sql.{SaveMode, SparkSession}
//import org.apache.spark.streaming.kafka010.ConsumerStrategies._
//import org.apache.spark.streaming.kafka010.LocationStrategies._
//import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//import scala.collection.mutable.ArrayBuffer
//
//
//// phoenixTableAsDataFrame
//import scala.collection.mutable
//
///**
//  * Created by chailei on 18/7/12.
//  */
//object OracleToHbaseComputeGroup {
//
//  def main(args: Array[String]) {
//
//    val conf = new SparkConf()
//      .setAppName("TestApp")
//      .setMaster("local[4]")
//        .set("spark.streaming.kafka.maxRatePerPartition","5")
//    //      .set("spark.executor.extraJavaOptions", "-XX:+UseConcMarkSweepGC")
//    //      .set("spark.streaming.kafka.maxRatePerPartition", "1000")
//          .set("spark.default.parallelism", "10")
//    //      "metadata.broker.list" -> "192.168.8.155:9092,192.168.8.156:9092,192.168.8.157:9092"
//
//    // metadata.broker.list 参数为什么不能用ip:端口
//    val paramMap = Map[String, Object](
//      "bootstrap.servers" -> "biaoyuan01:9092,biaoyuan02:9092,biaoyuan03:9092",
//      "key.deserializer" -> classOf[StringDeserializer],
//      "value.deserializer" -> classOf[StringDeserializer],
//      "group.id" -> "test",
//      "auto.offset.reset" -> "latest",
//      "enable.auto.commit" -> (false: java.lang.Boolean),
//      "enable.auto.commit" -> (false: java.lang.Boolean),
//      "max.partition.fetch.bytes" -> (2621440: java.lang.Integer), //default: 1048576
//      "request.timeout.ms" -> (90000: java.lang.Integer), //default: 60000
//      "session.timeout.ms" -> (60000: java.lang.Integer) //default: 30000
//    )
//    //"metadata.broker.list" -> "mini2:9092,mini3:9092,mini4:9092"
//    val topics = "bioqas"
//    val topicSet = topics.split(",")
//    val ssc = new StreamingContext(conf, Seconds(60))
//
//    val kafkaStream = KafkaUtils
//      .createDirectStream[String, String](
//      ssc,
//      PreferConsistent,
//      Subscribe[String, String](topicSet, paramMap)
//    )
//
//    val accu = ssc.sparkContext.longAccumulator("myflag")
//    val rddaccu = ssc.sparkContext.longAccumulator("count")
//    val myAcc = new IdAccumulator
//    val idAcc = new IdAccumulator
//    ssc.sparkContext.register(myAcc,"myAcc")
//    ssc.sparkContext.register(idAcc,"idAcc")
//    val ids = mutable.HashSet[String]()
//    val idBroadCast = ssc.sparkContext.broadcast(ids)
//    JdbcDialects.registerDialect(OracleDialect)
//    kafkaStream.foreachRDD(rdd => {
//
//      if (rdd != null) {
//
//        val offsetRangs = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//
//        idBroadCast.value.clear()
//
//        rdd.foreachPartition(partitions => {
//
//
//          if (partitions != null) {
//
//
//            partitions.foreach(line => {
//
//                val key = JsonParseGroupUtil.parseJson(line.value())
//
//              idAcc.add(key)
//
//            })
//
//
//
//          }
//        })
//
//        if(!"".equals(idAcc.value)){
//
//          println("id " + idAcc.value)
//          val ss = SparkSession.builder().config(conf).getOrCreate()
//          val groupId = mutable.StringBuilder.newBuilder
//            val id = mutable.StringBuilder.newBuilder
//          for(x: String <- idAcc.value.split(",").toSet){
//            val split: Array[String] = x.split("_")
//            groupId.append("plan_id=").append(split(0))
//              .append(" and ").append("times_id=").append(split(1))
//              .append(" and ").append("PROJECT_ID=").append(split(2)).append(" or ")
//
//            val key = split(0) + "%" + split(1) + "_"+ split(2)
//            id.append("id like ").append("\'").append("%").append(key).append("%").append("\'").append(" or ")
//          }
//          groupId.delete(groupId.length-4,groupId.length-1)
//          id.delete(id.length-4,id.length-1)
//          val tUserDF = ss.read.format("jdbc").options(Map(
//            "url" -> "jdbc:oracle:thin:@192.168.8.108:1521:orcl",
//            "user" -> "bioqas",
//            "password" -> "bioqas",
//            "dbtable" -> s"(select * from BIOQAS.T_SJ_TARGET_GROUP where $groupId) t",
//            "driver" -> "oracle.jdbc.driver.OracleDriver"
//          )).load()
//
//          tUserDF.show()
//          tUserDF.createOrReplaceTempView("groupData")
//
//
//  //        println("accu = " + accu.value)
//  //
//  //        for(x:String <- myAcc.value.split(",").toSet){// 这里不加String类型会报错
//  //          val split: Array[String] = x.split("_")
//  //
//  //          val key = split(0) + "%" + split(2) + split(3)
//  //          id.append("id like ").append("\'").append("%").append(key).append("%").append("\'").append(" or ")
//  //        }
//  //        id.delete(id.length-4,id.length-1)
//          println("id " + id.toString())
//          println("myAcc " + myAcc.value + "   " + myAcc.value.equals(""))
//          println("broadcast = " + idBroadCast.value.size)
//          //        if(idBroadCast.value.size != 0){
//  //        if(!myAcc.value.equals("")){// 当没有数据的时候，不读取数据进行计算
//          val biaoshi = "%"
//          ss.sqlContext.udf.register("middleValue",new MiddleValueUDAF())
//          val phoenixDF = ss.sqlContext.phoenixTableAsDataFrame(
//            "TP_E_BD_REPORT_DATE_BUCKET",    //TP_E_BD_REPORT_DATE_BUCKET
//            Seq("ID","CENTER_ID","PLAN_ID","SPEC_ID","ITEM_ID","BATCH_ID","TIMES_ID",
//              "CAL_MATERIAL_BRANDS_ID","LAB_ID","REAGENT_NAME_ID","REPORT_DATA"),// 字段必须是大写
//            Some(s"${id}"),//Some(s"ID like \'${biaoshi}${id}\'"),
//            Some("biaoyuan01:2181")
//          ).cache()
//          phoenixDF.printSchema()
//          ss.sqlContext.udf.register("middleValue",new MiddleValueUDAF())
//          val dataDF = phoenixDF.filter("REPORT_DATA!=999d")
//            .select("ID","CENTER_ID","PLAN_ID","SPEC_ID","ITEM_ID","TIMES_ID","BATCH_ID",
//              "CAL_MATERIAL_BRANDS_ID","LAB_ID","REAGENT_NAME_ID","REPORT_DATA")
//            .cache()
//
//          dataDF.show()
//          dataDF.createOrReplaceTempView("originData")
//
//          val totalDF = ss.sql("select o.*,g.group_id,g.group_name from originData o join groupData g on " +
//            "o.PLAN_ID = g.PLAN_ID and o.TIMES_ID = g.TIMES_ID and o.ITEM_ID = g.PROJECT_ID")
//
//          totalDF.show()
//
//          totalDF.groupBy("group_id").count().foreachPartition(line =>{
//            line.foreach(println)
//          })
//
//
//
//
//  //          println("total = " + dataDF.count())
//
//
//          totalDF.createOrReplaceTempView("tp")
//
//
//          val df = ss.sql("select tt.*,tp.REPORT_DATA from tp join (select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,METHOD_ID," +
//            "TIMES_ID,BATCH_ID,group_id,count(BATCH_ID),mean(REPORT_DATA) as mn,middleValue(REPORT_DATA) as middle,stddev(REPORT_DATA) " +
//            "as sd from tp group by CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,METHOD_ID,TIMES_ID,BATCH_ID,group_id) tt on " +
//            "tp.CENTER_ID = tt.CENTER_ID and tp.PLAN_ID = tt.PLAN_ID and tp.SPEC_ID = tt.SPEC_ID and " +
//            "tp.ITEM_ID = tt.ITEM_ID and tp.METHOD_ID = tt.METHOD_ID and tp.TIMES_ID = tt.TIMES_ID " +
//            "and tp.BATCH_ID = tt.BATCH_ID").cache()
//
//          df.show()
//
//
//          df.createOrReplaceTempView("compute")
//          df.show()
//
//          val p_3sd = ss.sql("select * from compute where mn <= (mn + 3*sd) and mn >= (mn - 3*sd)")
//
//          val p_2sd = ss.sql("select * from compute where mn <= (mn + 2*sd) and mn >= (mn - 2*sd)")
//
//          val total = df.union(p_2sd).union(p_3sd)
//
//          total.createOrReplaceTempView("total")
////          println("total = " + total.count())
//          //CENTER_ID|PLAN_ID|SPEC_ID|ITEM_ID|METHOD_ID|TIMES_ID|BATCH_ID|
//          val mmDF = ss.sql("select CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,METHOD_ID,TIMES_ID,BATCH_ID,group_id,count(BATCH_ID)" +
//            ",max(mn),min(mn),max(middle),min(middle) " +
//            "from total group by CENTER_ID,PLAN_ID,SPEC_ID,ITEM_ID,METHOD_ID,TIMES_ID,BATCH_ID,group_id")
//          import ss.implicits._
//
//          val mmRdd = mmDF.rdd.map(line => {
//            val array = new ArrayBuffer[Double]()
//            array.append(line.getAs[Double](9))
//            array.append(line.getAs[Double](10))
//            array.append(line.getAs[Double](11))
//            array.append(line.getAs[Double](12))
//            val max = array.max
//            val min = array.min
//            (line.getAs[Integer](0),line.getAs[Integer](1),line.getAs[Integer](2),line.getAs[Integer](3)
//              ,line.getAs[Integer](4),line.getAs[Integer](5),line.getAs[Integer](6),line.getAs[Integer](7),max.formatted("%.2f"),min.formatted("%.2f"))
//          }).toDF("CENTER_ID","PLAN_ID","SPEC_ID","ITEM_ID","TIMES_ID","BATCH_ID","group_id","max","min")
//
//
//          println("======================================")
//
//
//
//          val targetRDD = mmRdd.rdd.map(line => ((line.getAs[Integer](0),line.getAs[Integer](1),line.getAs[Integer](2),line.getAs[Integer](3),
//          line.getAs[Integer](4),line.getAs[Integer](5),line.getAs[Integer](6),line.getAs[Integer](7)),
//            (line.getAs[String](9).toDouble to line.getAs[String](8).toDouble by 0.1)))
//              .map(lines => {
//                val sb = mutable.StringBuilder.newBuilder
////                val arr = new  ArrayBuffer()
//                for(value <- lines._2){
//                  sb.append(lines._1._1).append(",")
//                    .append(lines._1._2).append(",")
//                    .append(lines._1._3).append(",")
//                    .append(lines._1._4).append(",")
//                    .append(lines._1._5).append(",")
//                    .append(lines._1._6).append(",")
//                    .append(lines._1._7).append(",")
//                    .append(lines._1._8).append(",")
//                    .append(value).append("\t")
//                  rddaccu.add(1)
//                }
//
//                sb.toString()
//              }).flatMap(values => {
//            values.split("\t")
//          }).cache()
//
//
//          val totalTargetDF = targetRDD.map(values => {
//            val targetValues = values.split(",")
//            (targetValues(0),targetValues(1),targetValues(2),
//              targetValues(3),targetValues(4),targetValues(5),targetValues(6),targetValues(7),targetValues(8))
//          }).toDF("CENTER_ID","PLAN_ID","SPEC_ID","ITEM_ID","TIMES_ID","BATCH_ID","group_id","target")
//
//          totalTargetDF.show()
//          totalTargetDF.createOrReplaceTempView("target")
//
//          val computeDF = ss.sql("select target.*,tp.REPORT_DATA,tp.LAB_ID from tp join target on " +
//                "tp.CENTER_ID = target.CENTER_ID and tp.PLAN_ID = target.PLAN_ID and tp.SPEC_ID = target.SPEC_ID and " +
//                "tp.ITEM_ID = target.ITEM_ID and tp.TIMES_ID = target.TIMES_ID " +
//                "and tp.BATCH_ID = target.BATCH_ID and tp.group_id = target.group_id")
//
//          computeDF.show()
//
//          val pjbz = 10
//          computeDF.printSchema()
//          val scoreDF = computeDF.rdd.map(line => {
//            val report_data = line.getAs[Double](9)
//            val target = line.getAs[String](8).toDouble
//            var score = 0
//            if(report_data >= (target - pjbz) && report_data <= (target + pjbz)){
//                score = 1
//            }
//            (line.getAs[String](0),line.getAs[String](1),line.getAs[String](2),line.getAs[String](3),
//              line.getAs[String](4),line.getAs[String](5),line.getAs[String](6),line.getAs[String](7),line.getAs[String](8).toDouble.formatted("%.2f"),line.getAs[Double](9),score,line.getAs[Integer](10))
//          }).toDF("CENTER_ID","PLAN_ID","SPEC_ID","ITEM_ID","TIMES_ID","BATCH_ID","group_id","target","REPORT_DATA","score","LAB_ID")
//
////          scoreDF.repartition(1).write.format("text").mode(SaveMode.Overwrite).save("/Users/chailei/project/idea/Bioqas/output/1")
//
//          scoreDF.show()
//          scoreDF.createOrReplaceTempView("scoreDF")
//
//
//         val resultDF =  ss.sql("select tt.CENTER_ID, tt.PLAN_ID,tt.SPEC_ID, tt.ITEM_ID, tt.METHOD_ID, tt.TIMES_ID, " +
//            "tt.BATCH_ID,tt.group_id, tt.target, tt.totalNum, tt.num1, tt.num2, tt.num1/totalNum as per1,tt.num2/totalNum as per2 from (select CENTER_ID, PLAN_ID,SPEC_ID, ITEM_ID, METHOD_ID, TIMES_ID, " +
//            "BATCH_ID,group_id, target,count(1) as totalNum, count(if(score = 1,1,null)) as num1,count(if(score = 0,1,null)) as num2 " +
//            "from scoreDF group by CENTER_ID, PLAN_ID,SPEC_ID, ITEM_ID, METHOD_ID, TIMES_ID,group_id, " +
//            "BATCH_ID, target) tt")//repartition(1).write.format("text").mode(SaveMode.Overwrite).save("/Users/chailei/project/idea/Bioqas/output/2")
//
//          resultDF.show(500)
//
//  //        resultDF.createOrReplaceTempView("test")
//  //        ss.sql("select * from test where target = 113.30 or target = 113.40 or target = 113.20").show()
//  //
//  //          println("total = " + dataDF.count() + " compute = " + computeDF.count() + "  totalTarget = " + totalTargetDF.count() +"  score = " + scoreDF.count() + "  result = " + resultDF.count())
//
//  //          ss.sql("select CENTER_ID, PLAN_ID, SPEC_ID, ITEM_ID, METHOD_ID, TIMES_ID, BATCH_ID, " +
//  //            "target,count(1) as tcn from scoreDF group by CENTER_ID, PLAN_ID, " +
//  //            "SPEC_ID, ITEM_ID, METHOD_ID, TIMES_ID, BATCH_ID,target").show()
//  //
//  //
//  //          ss.sql("select CENTER_ID, PLAN_ID, SPEC_ID, ITEM_ID, METHOD_ID, " +
//  //            "TIMES_ID, BATCH_ID, target, count(1) as scn from scoreDF where score = 1 group " +
//  //            "by CENTER_ID, PLAN_ID, SPEC_ID, ITEM_ID, METHOD_ID, TIMES_ID, BATCH_ID, target, score").show()
//
//  //          root
//  //          |-- ID: string (nullable = true)
//  //          |-- CENTER_ID: integer (nullable = true)
//  //          |-- PLAN_ID: integer (nullable = true)
//  //          |-- SPEC_ID: integer (nullable = true)
//  //          |-- ITEM_ID: integer (nullable = true)
//  //          |-- BATCH_ID: integer (nullable = true)
//  //          |-- TIMES_ID: integer (nullable = true)
//  //          |-- CAL_MATERIAL_BRANDS_ID: integer (nullable = true)
//  //          |-- METHOD_ID: integer (nullable = true)
//  //          |-- LAB_INS_ID: integer (nullable = true)
//  //          |-- REAGENT_NAME_ID: integer (nullable = true)
//  //          |-- REPORT_DATA: double (nullable = true)
//
//  //          total.show()
//
//            }
////        }
//
//        accu.reset()
//        myAcc.reset()
//        rddaccu.reset()
//        kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRangs)
//
//      }
//
//
//    })
//
//    println("================================")
//
//
//
//    ssc.start()
//    ssc.awaitTermination()
//
//
//
//  }
//
//}
