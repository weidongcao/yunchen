package com.rainsoft.test.scala

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017-04-25.
  */
object HiveContextOnYarnTest {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "root")
        val conf = new SparkConf()
            .setAppName("测试Spark的HiveContext")
            .setMaster("yarn-client")
//            .setMaster("local")
            .set("spark.yarn.dist.files", "D:\\0WorkSpace\\JetBrains\\yunchen\\src\\resource\\yarn-site.xml")
            .set("spark.yarn.jar", "hdfs://dn1.hadoop.com:8020/user/spark/share/lib/spark-assembly-1.5.0-hadoop2.6.0-cdh5.6.1.jar")
        val sc = new SparkContext(conf)
        sc.addJar("D:\\0WorkSpace\\JetBrains\\yunchen\\out\\artifacts\\hiveContext\\yunchen.jar")

//        val dataRDD = sc.textFile(args(0))
//        val length = dataRDD.count()

        val hiveContext = new HiveContext(sc)

        val userDF = hiveContext.sql("select * from user")
//        val userDF = hiveContext.sql("select * from temp")

        val length = userDF.count()

        println("length = " + length)
    }

}
