package com.bioqas.test

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

/**
  * Created by chailei on 18/6/8.
  */
object StatTestApp {



  def main(args: Array[String]) {



    val ss = SparkSession.builder()
      .appName("Stat")
      .master("local[3]")
      .getOrCreate()



    val lines = ss.sparkContext.textFile("/Users/chailei/antu/bioqas/test.txt")


    val schemes = "name,score"

    val fields = schemes.split(",")
      .map(filedname => StructField(filedname,StringType,true))


    val schema = StructType(fields)

    val rowRDD = lines.map(_.split(" ")).map(attr => Row(attr(0),attr(1)))


    val df = ss.createDataFrame(rowRDD,schema)


    df.show()
    df.distinct().show()//去重操作

//    df.describe().show()


//    df.select("name","score").groupBy("name")
    df.createOrReplaceTempView("tt")

    ss.sqlContext.udf.register("middleValue",new MiddleValueUDAF())

    ss.sql("select mid,var,sd,mx,mi,mn,sd/mn from (select name,middleValue(score) " +
      "as mid,variance(score) as var,stddev(score) as sd,max(score) as mx,min(score) " +
      "as mi,mean(score) as mn,count(name) from tt group by name)").show()

    import ss.implicits._

    // Map会去重，导致统计结果只有score min
    df.groupBy("name").agg(Map("score"->"max","name"->"count","score"->"min")).show()

    df.groupBy("name").agg("score"->"middleValue").show()

    df.groupBy("name").agg("score"->"max","name"->"count","score"->"min","score" -> "middleValue").show()

    df.groupBy("name").agg(("score","max"),("name","count"),("score","min"),("score" ,"middleValue")).show()

    df.map(x =>(x.getAs[String](1),1))


//    df.groupBy("name").agg(max("name")).show()

//    df.groupBy("name")

//    val obser = lines

//    println("最大值："+summer.max)
//    println("最小值"+summer.min)
//    println("方差"+summer.variance)
//    println("均值"+summer.mean)




//    lines.foreach(println)




    ss.stop()


  }

}
