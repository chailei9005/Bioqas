package com.bioqas.test

import org.apache.spark.sql.SparkSession

/**
  * Created by chailei on 18/7/19.
  */
object CheckStatApp {

  def main(args: Array[String]) {




    val spark = SparkSession.builder()
      .appName("TestEQA")
      .master("local[2]")
      .getOrCreate()
    spark.sqlContext.udf.register("middleValue",new MiddleValueUDAF())
    val totalDF = spark.read.format("csv").option("header", true).option("encoding","gbk").load("file:////Users/chailei/antu/EQA_zj/csvdata/EQA_zj-PTTestResult.csv")

    totalDF.printSchema()
    totalDF.createOrReplaceTempView("total")

    val specDF = spark.read.format("csv").option("header", true).option("encoding","gbk").load("file:////Users/chailei/antu/EQA_zj/csvdata/EQA_zj-Speciality.csv")

    specDF.printSchema()
    specDF.createOrReplaceTempView("spec")

    val itemDF = spark.read.format("csv").option("header", true).option("encoding","gbk").load("file:////Users/chailei/antu/EQA_zj/csvdata/EQA_zj-item.csv")

    itemDF.printSchema()
    itemDF.createOrReplaceTempView("item")


    val spec_item = spark.sql("select spec.code,spec.CName,item.itemindex as ii,item.Speciality" +
      ",item.Cname as ccname from spec join item on item.Speciality = spec.code")

    spec_item.show(100,true)



    spec_item.createOrReplaceTempView("specItem")

    val totalAllDF = spark.sql("select total.*,si.* from total join specItem si on  total.itemindex = si.ii")


    totalAllDF.show(500,true)

    println("totalAllDF = " + totalAllDF.count())


    totalAllDF.createOrReplaceTempView("totalAll")

    // | PTIndex|LabIndex|PTMaterialIndex|itemindex|result|code|CName|itemindex|Speciality|cname|

    val resultDF = spark.sql("select Speciality,CName,itemindex,ccname,PTMaterialIndex,count(PTMaterialIndex) as cn" +
      ",CAST (mean(result) as DECIMAL(13,2)) as mn,CAST (middleValue(result) as DECIMAL(13,2)) as middle,CAST (stddev(result) as DECIMAL(13,2)) as sd,CAST ((stddev(result)/mean(result)) as DECIMAL(13,4))*100 as cv," +
      "max(result) as max,min(result) as min " +
      "from totalALL where group by Speciality,CName,ccname,itemindex,PTMaterialIndex").cache()


    resultDF.show(100)

    resultDF.createOrReplaceTempView("result")

    val ptotal = spark.sql("select rt.*,ta.result from result rt join totalAll ta on rt.Speciality = ta.Speciality and rt.CName = ta.CName " +
      "and rt.ccname = ta.ccname and rt.itemindex = ta.itemindex and rt.PTMaterialIndex = ta.PTMaterialIndex")

    ptotal.createOrReplaceTempView("ptotal")

    val p3sd = spark.sql("select Speciality,CName,ccname,itemindex,PTMaterialIndex,count(PTMaterialIndex) as cn" +
      ",CAST (mean(result) as DECIMAL(13,2)) as mn,CAST (middleValue(result) as DECIMAL(13,2)) as middle," +
      "CAST (stddev(result) as DECIMAL(13,2)) as sd,CAST ((stddev(result)/mean(result)) as DECIMAL(13,4)) as cv," +
      "max(result) as max,min(result) as min from ptotal where result <= (mn + 3*sd) and result >= (mn - 3*sd) group by " +
      "Speciality,CName,ccname,itemindex,PTMaterialIndex")

    p3sd.createOrReplaceTempView("p3sd")

    val p2sd = spark.sql("select Speciality,CName,ccname,itemindex,PTMaterialIndex,count(PTMaterialIndex) as cn" +
      ",CAST (mean(result) as DECIMAL(13,2)) as mn,CAST (middleValue(result) as DECIMAL(13,2)) as middle," +
      "CAST (stddev(result) as DECIMAL(13,2)) as sd,CAST ((stddev(result)/mean(result)) as DECIMAL(13,4)) as cv," +
      "max(result) as max,min(result) as min from ptotal where result <= (mn + 2*sd) and result >= (mn - 2*sd) group by " +
      "Speciality,CName,ccname,itemindex,PTMaterialIndex")

    p2sd.createOrReplaceTempView("p2sd")

    spark.sql("select * from ptotal where Speciality = 05 and itemindex = 0501").show(100,true)
    spark.sql("select * from p3sd where Speciality = 05 and itemindex = 0501").show(100,true)
    spark.sql("select * from p2sd where Speciality = 05 and itemindex = 0501").show(100,true)

//    spark.sql("select * from totalAll where Speciality = 05 and itemindex = 0501 and PTMaterialIndex = 050309").show(100,true)


    println("total = " + totalDF.count())


    //    totalDF.show(false)

    //    totalDF.createOrReplaceTempView("tt")

    //    spark.sql("select * from tt where PTMaterialIndex = 070701").show()

    //    spark.sql("select count(*) from tt group by itemindex").show()


    //    proviceDF.write.format("csv").option("encoding","utf-8").save("file:///Users/chailei/antu/csvoutput")


    spark.stop()


  }
}
