package com.bioqas.utils

import com.bioqas.test.Configs
import com.google.gson.{JsonElement, JsonParser}


/**
  * Created by chailei on 18/8/16.
  */
object JsonParseGroupUtil {


  def parseJson(line: String) ={


    try{


      val jsonObj = new JsonParser().parse(line).getAsJsonObject()

      println(jsonObj)

      if(jsonObj.get("op_type").getAsString.equalsIgnoreCase("i")){

        val table = jsonObj.get("table").toString.toLowerCase.replaceAll("\"","").replace(s"${Configs.getString(Constants.DB_USER)}.","")

        val after = jsonObj.get("after").toString

        //    val afterObj = new JsonParser().parse(after).getAsJsonObject()
        val dataElement = jsonObj.getAsJsonObject("after").entrySet().iterator()


        var key = ""

        var result = ""
        var plan_id = ""
        var time_id = ""
        var item_id = ""
        var EVA_STAND_IDENTY: String = ""
        var EVA_STAND_VALUE: String = ""
        var GROUP_NODE_ID = ""

        while (dataElement.hasNext){

          val element = dataElement.next()
          key = element.getKey
          if(key.equalsIgnoreCase("ID")){
            result = element.getValue.toString.replaceAll("\"","")
          }
          if(key.equalsIgnoreCase("PLAN_ID")){
            plan_id = element.getValue.toString.replaceAll("\"","")
          }

          if(key.equalsIgnoreCase("PLAN_TIMES_ID")){
            time_id = element.getValue.toString.replaceAll("\"","")
          }
          if(key.equalsIgnoreCase("PROJECT_ID")){
            item_id = element.getValue.toString.replaceAll("\"","")
          }

          if(key.equalsIgnoreCase("EVA_STAND_IDENTY")){
            if(!element.getValue.isJsonNull){
              EVA_STAND_IDENTY = element.getValue.getAsString.replaceAll("\"","")
            }else{
              EVA_STAND_IDENTY = null
            }
          }
          if(key.equalsIgnoreCase("EVA_STAND_VALUE")){
            EVA_STAND_VALUE = element.getValue.toString.replaceAll("\"","")
          }
          if(key.equalsIgnoreCase("GROUP_NODE_ID")){
            GROUP_NODE_ID = element.getValue.toString.replaceAll("\"","")
          }

        }

        println(plan_id + "_" + time_id + "_" + item_id + "," + EVA_STAND_IDENTY + "_" +
        EVA_STAND_VALUE + "_" + GROUP_NODE_ID)
//        println(EVA_STAND_IDENTY.toString)
//        println("11"+EVA_STAND_IDENTY+"11")
//        println(None.getOrElse(EVA_STAND_IDENTY,false))

        println(EVA_STAND_IDENTY != null)


        if(EVA_STAND_IDENTY != null){
          println(result + "_" + time_id + "_" + item_id + "-" + result + time_id + item_id + "_" + EVA_STAND_IDENTY + "_" + EVA_STAND_VALUE)
          result + "_" + time_id + "_" + item_id + "-" + result + time_id + item_id + "_" + EVA_STAND_IDENTY + "_" + EVA_STAND_VALUE
        } else {
          ""
        }

      } else {
        ""
      }

    }catch {
      case e: Exception => e.printStackTrace()
      ""
    }



  }

}
