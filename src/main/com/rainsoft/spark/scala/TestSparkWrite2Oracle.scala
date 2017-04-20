package com.rainsoft.spark.scala

import java.util.Properties

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017-03-24.
  */
object TestSparkWrite2Oracle {
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

    val datas = Array(
      ("460001", "130001", "江苏南京", "000001", "1", "000001"),
      ("460002", "130002", "浙江杭州", "000002", "2", "000002"),
      ("460003", "130003", "广东深圳", "000003", "3", "000003")
    )
    val originalRDD = sc.parallelize(datas)

    val dataRDD = originalRDD.map(tuple => {
      Row.apply(
        Row(tuple._1),
        Row(tuple._2),
        Row(tuple._3),
        Row(tuple._4),
        Row(tuple._5),
        Row(tuple._6)
      )
    })
    val schema = StructType(
      StructField("IMSI_NUM", StringType) ::
        StructField("PHONE_NUM", StringType) ::
        StructField("AREA_NAME", StringType) ::
        StructField("AREA_CODE", StringType) ::
        StructField("PHONE_TYPE", StringType) ::
        StructField("REGION", StringType) :: Nil
    )
    val dataDF = sqlContext.createDataFrame(dataRDD, schema)


    val prop = new Properties()
    prop.put("driver", "oracle.jdbc.driver.OracleDriver")

    prop.setProperty("user", USERNAME)
    prop.setProperty("password", PASSWORD)
    //    dataDF.write.mode("append").jdbc(uri, TABLE_NAME, prop)

//    dataDF.write.options(jdbcMap).format("jdbc").insertInto(TABLE_NAME)

//    dataDF.write.mode("append").format("jdbc").options(jdbcMap)
    val properties=new Properties()
    properties.setProperty("user",USERNAME)
    properties.setProperty("password", PASSWORD)
    val temp = dataDF.write.mode("append")
//    val temp1 = temp.jdbc(DATABASE, TABLE_NAME, properties)
    val temp1 = temp.jdbc(DATABASE, "sys_phone_to_area", properties)
  }
}
