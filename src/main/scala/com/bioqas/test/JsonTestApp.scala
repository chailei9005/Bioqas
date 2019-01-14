package com.bioqas.test

import com.google.gson.JsonParser

/**
  * Created by chailei on 18/9/12.
  */
object JsonTestApp {

  def main(args: Array[String]) {


    val json =
      """
        |[{
        |	"label": "锡林浩特",
        |	"specialLabel": "",
        |	"type": 1,
        |	"value": "2400",
        |	"gType": 2,
        |	"percentageUser": "",
        |	"pingYin": "",
        |	"hot": "",
        |	"labelDesc": "",
        |	"isSelected": false,
        |	"hotRecommend": "",
        |	"selected": false
        |}, {
        |	"label": "锡林郭勒",
        |	"specialLabel": "",
        |	"type": 42,
        |	"value": "266",
        |	"gType": 0,
        |	"percentageUser": "",
        |	"pingYin": "",
        |	"hot": "",
        |	"labelDesc": "",
        |	"isSelected": false,
        |	"hotRecommend": "",
        |	"selected": false
        |}, {
        |	"label": "",
        |	"specialLabel": "",
        |	"type": 47,
        |	"value": "2018-07-13,2018-07-14",
        |	"gType": 0,
        |	"percentageUser": "",
        |	"pingYin": "",
        |	"hot": "",
        |	"labelDesc": "",
        |	"isSelected": false,
        |	"hotRecommend": "",
        |	"selected": false
        |}]
      """.stripMargin

    val jsonObject = new JsonParser().parse(json).getAsJsonArray

    for(i <- 0 until jsonObject.size())
    println(jsonObject.get(i).getAsJsonObject.get("type") + "  " +
      jsonObject.get(i).getAsJsonObject.get("value").getAsString.replaceAll("\"",""))

  }

}
