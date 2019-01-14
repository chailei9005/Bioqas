package com.bioqas.test

import com.bioqas.utils.IdAccumulator
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by chailei on 18/6/28.
  */
object MyAccumulator {

  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName("Accumulator1").setMaster("local")
    val sc = new SparkContext(conf)

    val myAcc = new IdAccumulator
    sc.register(myAcc,"myAcc")

    //val acc = sc.longAccumulator("avg")
    val nums = Array("1","2","3","4","5","6","7","8")
    val numsRdd = sc.parallelize(nums)

    numsRdd.foreach(num => myAcc.add(num))
    println(myAcc)
    println(myAcc.value)
    sc.stop()



  }

}
