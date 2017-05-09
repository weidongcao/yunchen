package com.rainsoft.spark.scala

import java.io.File

import com.rainsoft.util.java.IMSIUtils
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by Administrator on 2017-04-27.
  */
object EmphasisAnalysis {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("重点区域人群分析Scala版本")

        val sc  = new SparkContext(conf)

        val sqlContext = new HiveContext(sc)

        val path = args(0)
        val bcpRDD = sc.textFile("file://" + path)
            .map(line =>{
                val fields = line.split("\\|$\\|")(0)
                val arr = fields.split("\\|#\\|")
                val phone7 =IMSIUtils.getMobileAll(arr(1))
                RowFactory.create(arr(0), arr(1), phone7, arr(2), arr(3), arr(4))
            })

        val bcpSchema = DataTypes.createStructType(Array(
            DataTypes.createStructField("equipment_mac", DataTypes.StringType, true),
            DataTypes.createStructField("imsi_code", DataTypes.StringType, true),
            DataTypes.createStructField("phone_num", DataTypes.StringType, true),
            DataTypes.createStructField("capture_time", DataTypes.StringType, true),
            DataTypes.createStructField("operator_type", DataTypes.StringType, true),
            DataTypes.createStructField("sn_code", DataTypes.StringType, true)
        ))

        val bcpDF = sqlContext.createDataFrame(bcpRDD, bcpSchema)

        println("-------------------------------------------" + System.getProperty("user.dir") + "-------------------------------------------")

        bcpDF.registerTempTable("improve")
        val sql = FileUtils.readFileToString(new File("emphasisAnalysis.sql"))

        println("-----------------------------------------")
        println("采集信息表与手机信息表join的sql = " + sql)
        println("-----------------------------------------")
        import sqlContext.implicits._
        sqlContext.sql("use yuncai")
        sqlContext.sql(sql).collect().foreach(println)

    }

}
