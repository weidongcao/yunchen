package com.rainsoft.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017-04-18.
  */
object TestFilter {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("filter")
            .setMaster("local")

        val sc = new SparkContext(conf)



    }

}
