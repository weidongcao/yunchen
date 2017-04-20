package com.rainsoft.spark.scala

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by Administrator on 2017-04-07.
  */
object BcpChat {
    //    val filePath = "D:\\Program Files\\Java\\JetBrains\\workspace\\bigdata\\src\\main\\zdata\\bcp\\testdata"
    val filePath = "D:\\Program Files\\Java\\JetBrains\\workspace\\bigdata\\src\\main\\zdata\\bcp\\"

    def main(args: Array[String]): Unit = {
        val dir = new File(filePath)
        val fileList = dir.listFiles().filter(_.isFile).filter(_.toString.endsWith("bcp"))

        fileList.foreach(println)
        //        val data = convertFilContext(filePath)
        //        data.foreach(println)

    }


}
