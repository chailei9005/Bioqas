package com.bioqas.test



/**
  * Created by chailei on 18/7/20.
  */
object MiddleCheckApp {

  def main(args: Array[String]) {

    val ll =List(1.0,2.0,3.0,4.0)

    val list = List[Double](308.8,
      299,
      302.3,
      301.2,
      249.5,
      350.9,
      361.5,
      286.6,
      210,
      235.4,
      283.4,
      260.1,
      258,
      239,
      285.1,
      255,
      229.2,
      257.2,
      245,
      204.6,
      252.8,
      303.6,
      198.6,
      265.2,
      284.5,
      279,
      276,
      259.9,
      268.2,
      249.8,
      284.8,
      319.5,
      259.7,
      332.7,
      262.9,
      262,
      261.4,
      277.7,
      270.8,
      238,
      263.6,
      263.2,
      253.4,
      262.4,
      318.4,
      275.9,
      281.1,
      331.2,
      231.4,
      236.4,
      343.1,
      302.3,
      256.3,
      354.2,
      287.6,
      301,
      290.2,
      278.5,
      311.7,
      302.3,
      301.6,
      361.4,
      284.6,
      300,
      262.6,
      287.8,
      313.3,
      214.1,
      296.5,
      243.1,
      290.6,
      248,
      286.8,
      219.5,
      296.6,
      210.6)

    println(middle(list))
  }

  def middle(list1:List[Double]):Double={
    val len = list1.size
    println("len = " + len)
    val list = list1.sorted
    var mid = 0d
    println("one index= " + (len / 2 - 1) + " two index = " + (len / 2))
    println("one = " + list(len / 2 - 1) + " two = " + list(len / 2))
    if (len % 2 == 0)
    mid = (list(len / 2 - 1) + list(len / 2)) / 2
    else
      mid = list(len / 2)
    mid
  }

}
