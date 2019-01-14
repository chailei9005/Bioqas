package com.bioqas.utils

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * Created by chailei on 18/6/28.
  */
class IdAccumulator extends AccumulatorV2[String,String]{

//  private var result = ""
  private var set = mutable.HashSet[String]()

  override def isZero: Boolean = {
//    result.equals("")
    set.isEmpty
  }

  override def merge(other: AccumulatorV2[String, String]): Unit = {
    other match {
//      case o : IdAccumulator => result += o.result
      case o : IdAccumulator => set ++=o.set

      case _ => throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }
  }

  override def copy(): AccumulatorV2[String, String] = {
    val idAccumulator = new IdAccumulator
    idAccumulator.set = this.set
    idAccumulator
  }

  override def value: String = {
//    result
    set.mkString(",")
  }

  override def add(v: String): Unit = {
//    result += v + ","
    set.add(v)
  }

  override def reset(): Unit = {
//    result = ""
    set.clear()
  }
}
