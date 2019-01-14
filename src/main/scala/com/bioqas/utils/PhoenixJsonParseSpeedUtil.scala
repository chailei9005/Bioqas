package com.bioqas.utils

import com.google.gson.{JsonElement, JsonParser}
import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.sql.Row

/**
  *
  */
object PhoenixJsonParseSpeedUtil {



  def parseJSONToPhoenixSQL(row: Row) ={

    try{

      val id = "\'" + row.getAs[String](0) + "\'"
      val center_id = row.getAs[Integer](1)
      val plan_id = row.getAs[Integer](2)
      val spec_id = row.getAs[Integer](3)
      val times_id = row.getAs[Integer](4)
      val item_id = row.getAs[Integer](5)
      val batch_id = row.getAs[Integer](6)
      val count = row.getAs[Integer](7)
      val avg = row.getAs[Integer](8)
      val median = row.getAs[Integer](9)
      val sd = row.getAs[Integer](10)
      val cv = row.getAs[Integer](11)
      val max = row.getAs[Integer](12)
      val min = row.getAs[Integer](13)
      val group_type = row.getAs[Integer](14)
      val group_id = row.getAs[Integer](15)


      val sql = s"upsert into INSERT_SPEED(ID,CENTER_ID,PLAN_ID,SPEC_ID,TIMES_ID,ITEM_ID,BATCH_ID," +
        "STATISTICS_COUNT,STATISTICS_AVG,STATISTICS_MEDIAN,STATISTICS_SD,STATISTICS_CV,STATISTICS_MAX," +
        s"STATISTICS_MIN,GROUP_TYPE,GROUPID) values(${id},${center_id},${plan_id},${spec_id}" +
        s",${times_id},${item_id},${batch_id},${count}" +
        s",${avg},${median},${sd},${cv}" +
        s",${max},${min},${group_type},${group_id})"

      sql


    }catch {
      case e:Exception => e.printStackTrace()
        ""
    }

  }
}
