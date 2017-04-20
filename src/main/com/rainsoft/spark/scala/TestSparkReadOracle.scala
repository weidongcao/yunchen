package com.rainsoft.spark.scala

import java.util.Properties

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017-03-24.
  */
object TestSparkReadOracle {
  val DATABASE = "jdbc:oracle:thin:@172.16.100.18:1521:orcl"
  val URL = "jdbc:oracle:yunan/yunan:thin:@172.16.100.18:1521:orcl"
  val USERNAME = "yunan"
  val PASSWORD = "yunan"
  val TABLE_NAME = "sys_imsi_key_area"
  val DRIVER = "oracle.jdbc.driver.OracleDriver"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("TestSpark2Oracle")
      .setMaster("local")
    val jdbcMap = Map("url" -> DATABASE,
                  "user" -> USERNAME,
                  "password" -> PASSWORD,
                  "dbtable" -> TABLE_NAME,
                  "driver" -> DRIVER)

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    /*val prop = new Properties()
    //注意：集群上运行时，一定要添加这句话，否则会报找不到mysql驱动的错误
    prop.put("driver", "oracle.jdbc.driver.OracleDriver")
    //加载mysql数据表
    val imsiDF: DataFrame = sqlContext.read.jdbc(URL, TABLE_NAME, prop)

    //从dataframe中获取所需字段
    imsiDF.select("phone_num", "area_name").collect()
      .foreach(row => {
        println("phone_num = " + row(0))
        println("area_name  " + row(1))
      })*/
    val jdbcDF = sqlContext.read.options(jdbcMap).format("jdbc").load
    jdbcDF.select("PHONE_NUM", "AREA_NAME").collect()
          .foreach(row => {
            println("phone_num = " + row(0))
            println("area_name  " + row(1))
          })


  }
}
