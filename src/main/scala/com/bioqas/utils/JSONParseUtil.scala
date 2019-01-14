package com.bioqas.utils

import com.bioqas.test.Configs
import com.google.gson.{JsonElement, JsonParser}
import org.apache.commons.codec.digest.DigestUtils

/**
  *
  */
object JSONParseUtil {

  // TP_E_BD_REPORT_DATE_BAK
  // {"table":"BIOQAS.TP_E_BD_REPORT_DATE","op_type":"U","op_ts":"2018-06-27 02:23:49.478359"
  // ,"current_ts":"2018-06-27T10:23:54.761000","pos":"00000000210000006185",
  // "before":{"ID":10139,"CENTER_ID":22,"PLAN_ID":168,"PLAN_NAME":"2017漯河市血细胞计数及常规化学室间质评活动",
  // "PLAN_CREATE_USER_ID":157,"SPEC_ID":57,"SPEC_NAME":"常规化学","TIMES_ID":410,"TIMES_NAME":"2","ITEM_ID":798,
  // "ITEM_NAME":"钾","ITEM_TYPE":1,"ITEM_UNITS_NAME":"mmol/L","BATCH_ID":1925,"BATCH_NAME":"20170922"
  // ,"REPORT_DATA":"3.74","METHOD_ID":155,"METHOD_NAME":"离子选择性电极(直接法)","LAB_INS_ID":547,
  // "LAB_INS_NAME":"HC-9885","INS_ID":97,"INS_NAME":"HC-9885","INS_BRANDS_ID":33
  // ,"INS_BRANDS_NAME":"深圳航创医疗设备有限公司","REAGENT_NAME_ID":662,"REAGENT_NAME":"其它","REAGENT_BRANDS_ID":33,
  // "REAGENT_BRANDS_NAME":"深圳航创医疗设备有限公司","CAL_MATERIAL_BRANDS_ID":602,"CAL_MATERIAL_BRANDS_NAME":"其他",
  // "INPUT_TYPE":1,"INPUT_DATE":"2017-09-21 10:22:52","INPUT_USER_ID":236,"REPORT_TYPE":1,
  // "REPORT_DATE":"2017-09-21 10:24:43","REPORT_USER_ID":236,"LAB_ID":181,"LAB_NAME":"舞阳县人民医院一分院检验科",
  // "LAB_CODE":null,"LAB_SPELL":"R","LAB_LONGITUDE":"113.611293","LAB_LATITUDE":"33.450522",
  // "CREATE_DATE":"2018-06-20 10:06:31","IS_DELETE":0,"DELETE_DATE":null},"after":{"ID":10139,"CENTER_ID":22,
  // "PLAN_ID":168,"PLAN_NAME":"2017漯河市血细胞计数及常规化学室间质评活动","PLAN_CREATE_USER_ID":157,"SPEC_ID":57,
  // "SPEC_NAME":"常规化学","TIMES_ID":410,"TIMES_NAME":"2","ITEM_ID":798,"ITEM_NAME":"钾","ITEM_TYPE":1,
  // "ITEM_UNITS_NAME":"mmol/L","BATCH_ID":1925,"BATCH_NAME":"20170922","REPORT_DATA":"3.74","METHOD_ID":155,
  // "METHOD_NAME":"离子选择性电极(直接法)","LAB_INS_ID":547,"LAB_INS_NAME":"HC-9885","INS_ID":97,"INS_NAME":"HC-9885",
  // "INS_BRANDS_ID":33,"INS_BRANDS_NAME":"深圳航创医疗设备有限公司","REAGENT_NAME_ID":662,"REAGENT_NAME":"其它",
  // "REAGENT_BRANDS_ID":33,"REAGENT_BRANDS_NAME":"深圳航创医疗设备有限公司","CAL_MATERIAL_BRANDS_ID":602,
  // "CAL_MATERIAL_BRANDS_NAME":"其他","INPUT_TYPE":1,"INPUT_DATE":"2017-09-21 10:22:52","INPUT_USER_ID":236,
  // "REPORT_TYPE":1,"REPORT_DATE":"2017-09-21 10:24:43","REPORT_USER_ID":236,"LAB_ID":181,
  // "LAB_NAME":"舞阳县人民医院一分院检验科","LAB_CODE":null,"LAB_SPELL":"R","LAB_LONGITUDE":"113.611293",
  // "LAB_LATITUDE":"33.450522","CREATE_DATE":"2018-06-20 10:06:31","IS_DELETE":0,"DELETE_DATE":null}}


  def parseJSONToPhoenixSQL(line:String) ={

    try{

      println(line)

      val jsonObj = new JsonParser().parse(line).getAsJsonObject()

      println(jsonObj.get("op_type").getAsString)
      if(jsonObj.get("op_type").getAsString.equalsIgnoreCase("i")){

        val table = jsonObj.get("table").toString.toLowerCase.replaceAll("\"","").replace(s"${Configs.getString(Constants.DB_USER)}.","")

        val after = jsonObj.get("after").toString

        // val afterObj = new JsonParser().parse(after).getAsJsonObject()
        val dataElement = jsonObj.getAsJsonObject("after").entrySet().iterator()

        // "ID":10139,"CENTER_ID":22,
        // "PLAN_ID":168,"PLAN_NAME":"2017漯河市血细胞计数及常规化学室间质评活动","PLAN_CREATE_USER_ID":157,"SPEC_ID":57,
        // "SPEC_NAME":"常规化学","TIMES_ID":410,"TIMES_NAME":"2","ITEM_ID":798,"ITEM_NAME":"钾","ITEM_TYPE":1,
        // "ITEM_UNITS_NAME":"mmol/L","BATCH_ID":1925,"BATCH_NAME":"20170922","REPORT_DATA":"3.74","METHOD_ID":155,
        // "METHOD_NAME":"离子选择性电极(直接法)","LAB_INS_ID":547,"LAB_INS_NAME":"HC-9885","INS_ID":97,"INS_NAME":"HC-9885",
        // "INS_BRANDS_ID":33,"INS_BRANDS_NAME":"深圳航创医疗设备有限公司","REAGENT_NAME_ID":662,"REAGENT_NAME":"其它",
        // "REAGENT_BRANDS_ID":33,"REAGENT_BRANDS_NAME":"深圳航创医疗设备有限公司","CAL_MATERIAL_BRANDS_ID":602,
        // "CAL_MATERIAL_BRANDS_NAME":"其他","INPUT_TYPE":1,"INPUT_DATE":"2017-09-21 10:22:52","INPUT_USER_ID":236,
        // "REPORT_TYPE":1,"REPORT_DATE":"2017-09-21 10:24:43","REPORT_USER_ID":236,"LAB_ID":181,
        // "LAB_NAME":"舞阳县人民医院一分院检验科","LAB_CODE":null,"LAB_SPELL":"R","LAB_LONGITUDE":"113.611293",
        // "LAB_LATITUDE":"33.450522","CREATE_DATE":"2018-06-20 10:06:31","IS_DELETE":0,"DELETE_DATE":null

        //    println("table : " + table)
        //    println("after : " + after)

        var key = ""
        var value : Any = null
        val keysStr = StringBuilder.newBuilder
        val valuesStr = StringBuilder.newBuilder

        val sql = StringBuilder.newBuilder

        var result = ""
        var plan_id = ""
        var spec_id = ""
        var time_id = ""
        var item_id = ""
        var batch_id = ""
        var lab_id = ""

        while (dataElement.hasNext){

          val element = dataElement.next()
          key = element.getKey
          if(key.equalsIgnoreCase("ID")){
            result = element.getValue.toString.replaceAll("\"","")
          }
          if(key.equalsIgnoreCase("PLAN_ID")){
            plan_id = element.getValue.toString.replaceAll("\"","")
          }
          if(key.equalsIgnoreCase("SPEC_ID")){
            spec_id = element.getValue.toString.replaceAll("\"","")
          }
          if(key.equalsIgnoreCase("TIMES_ID")){
            time_id = element.getValue.toString.replaceAll("\"","")
          }
          if(key.equalsIgnoreCase("ITEM_ID")){
            item_id = element.getValue.toString.replaceAll("\"","")
          }
          if(key.equalsIgnoreCase("BATCH_ID")){
            batch_id = element.getValue.toString.replaceAll("\"","")
          }
          if(key.equalsIgnoreCase("LAB_ID")){
            lab_id = element.getValue.toString.replaceAll("\"","")
          }

          if(key.equalsIgnoreCase("report_data")){
            //        println(element.getValue)
            //        println(null == null)
            //        println(element.getValue==null)
            //        println(element.getValue)
            val value1: JsonElement = element.getValue
            //        println(value1)
            if (value1.isJsonNull){// jsonElement 是 null 时 不能用 jsonElement == null 来判断是否为空
              //          println("report_data is null")
              value = 999d
            } else {
              //          println("report_data is not null")
              value = element.getValue.toString.replaceAll("\"","").toDouble
            }
          }else{
            value = element.getValue.toString.replaceAll("\"","\'")
          }
          //      println("value = " + value)
          keysStr.append(key).append(",")
          valuesStr.append(value).append(",")
        }

        keysStr.deleteCharAt(keysStr.length - 1)
        valuesStr.deleteCharAt(valuesStr.length - 1)

        val values = StringBuilder.newBuilder
        val split: Array[String] = valuesStr.toString().split(",")
        val rowkey: String = "\'" + DigestUtils.md5Hex(plan_id+spec_id+time_id+item_id+batch_id+lab_id).substring(0, 4) + "_" +
          plan_id + "_" + spec_id + "_" + time_id + "_" + item_id + "\'"
        split.update(0,rowkey)
        for(x <- split){
          values.append(x).append(",")
        }
        values.deleteCharAt(values.length - 1)


        sql.append("upsert into ").append(table.toUpperCase()+"_BUCKET")
          .append("(" + keysStr.toString() + ") values(")
          .append(values.toString() + ")")

        println("sql :" + sql.toString())


        //    println(rowkey.substring(6,rowkey.length-1))

        (sql.toString(),rowkey.substring(6,rowkey.length-1))
      } else {
        ("","")
      }

    }catch {
      case e:Exception => e.printStackTrace()
        ("","")
    }

  }
}
