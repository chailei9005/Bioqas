package com.bioqas.test

//import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.{StateSpec, State, Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

case class AfterRecord1(ID:Int,CENTER_ID:Int,PLAN_ID:Int,PLAN_NAME:String,PLAN_CREATE_USER_ID:Int,SPEC_ID:Int
                       ,SPEC_NAME:String,TIMES_ID:Int,TIMES_NAME:String,ITEM_ID:Int,ITEM_NAME:String,ITEM_TYPE:Int
                       ,ITEM_UNITS_NAME:String,BATCH_ID:Int,BATCH_NAME:String,REPORT_DATA:String,METHOD_ID:Int,METHOD_NAME:String
                       ,LAB_INS_ID:Int,LAB_INS_NAME:String,INS_ID:Int,INS_NAME:String,INS_BRANDS_ID:Int,INS_BRANDS_NAME:String
                       ,REAGENT_NAME_ID:Int,REAGENT_NAME:String,REAGENT_BRANDS_ID:Int,REAGENT_BRANDS_NAME:String,CAL_MATERIAL_BRANDS_ID:Int,CAL_MATERIAL_BRANDS_NAME:String
                       ,INPUT_TYPE:Int,INPUT_DATE:String,INPUT_USER_ID:Int,REPORT_TYPE:Int,REPORT_DATE:String,REPORT_USER_ID:Int
                       ,LAB_ID:Int,LAB_NAME:String,LAB_CODE:String,LAB_SPELL:String,LAB_LONGITUDE:String,LAB_LATITUDE:String
                       ,CREATE_DATE:String,IS_DELETE:Int,DELETE_DATE:String)

case class Record1(table:String,op_type:String,op_ts:String,current_ts:String,pos:String,after:AfterRecord1)



object StateBioqasApp {


  def main(args: Array[String]) {


    val conf = new SparkConf()
      .setAppName("StateBioqasApp")
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
    val ssc = new StreamingContext(conf,Seconds(20))
    val initialRDD = ssc.sparkContext.parallelize(List(("hello", 1), ("world", 1)))

    ssc.checkpoint("file:///Users/chailei/checkpoint")
    val kafkaStream = KafkaUtils
      .createDirectStream[String,String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicSet, paramMap)
    )


//    val wordDstream = kafkaStream.map(x => (x._2, 1))

    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
      val output = (word, sum)
      state.update(sum)
      output
    }

    val stateDstream = kafkaStream.mapPartitions(partition => {
      partition.map(value => {
        implicit val formats: DefaultFormats.type = DefaultFormats
        val json = parse(value.value())
        //id = record.after.ID
        //record
        json.extract[Record1]

      })
    }).map(x => (x.after.CENTER_ID + "\t" + x.after.PLAN_ID +"\t"+ x.after.SPEC_ID,1)).mapWithState(
      StateSpec.function(mappingFunc).initialState(initialRDD))

    stateDstream.print()


    ssc.start()
    ssc.awaitTermination()

  }

}
