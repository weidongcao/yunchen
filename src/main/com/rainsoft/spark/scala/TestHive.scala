package com.rainsoft.spark.scala

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017-03-24.
  */
object TestHive {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setAppName("test HiveContext")

    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    val arr = Array(4500, 4999, 2800, 2099, 1600, 799, 899, 499, 460, 650, 500)
    hiveContext.sql("use yuncai")
    val hiveRDD = hiveContext.sql("select * from h_community_analysis").rdd
    hiveRDD.foreach(row => println(row.toString()))
  }

}
