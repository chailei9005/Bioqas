package com.bioqas.test

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataTypes, StructType, DataType}


/**
  * Created by chailei on 18/6/8.
  */
class MiddleValueUDAF extends UserDefinedAggregateFunction{

//  val list1 = new StringBuffer()
//  val list = new StringBuffer()
  // 输入参数的数据类型
  override def inputSchema: StructType = {
     DataTypes.createStructType(util.Arrays
      .asList((DataTypes.createStructField("score",DataTypes.StringType,true))))
  }

  /**
    *
    * 更新 可以认为一个一个地将组内的字段值传递进来 实现拼接的逻辑
    * buffer.getInt(0)获取的是上一次聚合后的值
    * 相当于map端的combiner，combiner就是对每一个map task的处理结果进行一次小聚合
    * 大聚和发生在reduce端.
    * 这里即是:在进行聚合的时候，每当有新的值进来，对分组后的聚合如何进行计算
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
//    println("====1 " +buffer.get(0))// 初始值
//    println("----2 " +input.get(0)) // 每次输入的值
//    buffer.update(0,Integer.valueOf(buffer.get(0).toString)+Integer.valueOf(input.get(0).toString))

//    val ss = list1.append(input.get(0)).toString
//    println("sss "+ss)
    buffer.update(0,buffer.get(0)+","+input.get(0).toString)
//    println(list1.toString)
  }

  // buffer中的数据类型
  override def bufferSchema: StructType = {
     DataTypes.createStructType(util.Arrays
      .asList((DataTypes.createStructField("summ",DataTypes.StringType,true))))

  }

  /**
    * 合并其他部分结果
    * 合并 update操作，可能是针对一个分组内的部分数据，在某个节点上发生的 但是可能一个分组内的数据，会分布在多个节点上处理
    * 此时就要用merge操作，将各个节点上分布式拼接好的串，合并起来
    * buffer1.getInt(0) : 大聚合的时候 上一次聚合后的值
    * buffer2.getInt(0) : 这次计算传入进来的update的结果
    * 这里即是：最后在分布式节点完成后需要进行全局级别的Merge操作
    * 也可以是一个节点里面的多个executor合并
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
//    println("merge buffer1 "+Integer.valueOf(buffer1.get(0).toString))
//    println("merge buffer2 "+Integer.valueOf(buffer2.get(0).toString))

//    buffer1.update(0,Integer.valueOf(buffer1.get(0).toString)+Integer.valueOf(buffer2.get(0).toString))


    buffer1.update(0,buffer1.get(0)+","+buffer2.get(0).toString)
//    println(list.toString)

  }

  //初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0,"")
  }

  // 确保一致性 一般用true,用以标记针对给定的一组输入，UDAF是否总是生成相同的结果
  override def deterministic: Boolean = {

     true
  }

  override def evaluate(buffer: Row): Any = {
     val intArray = buffer.get(0).toString.replaceAll(",,",",").substring(1)
//    println("////////////// " + intArray)
    val list = intArray.split(",").map(_.toDouble).toList.sorted
    val len = list.size
//    println("size = " + len)
    var mid = 0d
    if (len % 2 == 0)
      mid = (list(len / 2 - 1) + list(len / 2)) / 2
    else
      mid = list(len / 2)
    mid
  }


  // 返回值的类型
  override def dataType: DataType = {

     DataTypes.DoubleType
  }



}
