package com.rainsoft.test.scala

import com.rainsoft.util.java.IMSIUtils
import org.apache.spark.sql.{RowFactory, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017-04-25.
  */
object SparkSQLTest {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("测试Spark的HiveContext")
            .setMaster("local")

        val sc = new SparkContext(conf)

        val sqlContext = new SQLContext(sc)

        val hiveContext = new HiveContext(sc)

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

        val bcpDF = hiveContext.createDataFrame(bcpRDD, bcpSchema)

        bcpDF.registerTempTable("improve")

        import hiveContext.implicits._
        hiveContext.sql("use yuncai")
        hiveContext.sql("select i.imsi_code, p.phone_num, p.area_name, p.area_code from improve i left join h_sys_phone_to_area p on i.phone_num = p.phone_num")


    }
}
