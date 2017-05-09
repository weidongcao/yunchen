package com.rainsoft.test.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017-04-26.
  */
object TestSpark {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("测试Spark集群是否能用")

        val sc = new SparkContext(conf)

        val dataRDD = sc.textFile(args(0))

        val length = dataRDD.count()

        println("-----------------------------------")
        println("length = " + length)
        println("-----------------------------------")
    }
}
