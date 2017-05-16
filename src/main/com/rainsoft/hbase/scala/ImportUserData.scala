package com.rainsoft.hbase.scala

import com.rainsoft.util.java.HBaseUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017-05-16.
  */
object ImportUserData {
    def main(args: Array[String]): Unit = {
        System.setProperty("user.name", "root")
        val conf = new SparkConf()
            .setAppName("Scala Import User Data")
            .setMaster("local")

        val sc = new SparkContext(conf)

        val originalRDD = sc.textFile("file:///D:\\0WorkSpace\\Develop\\data\\user_data.txt")
            .map(line => {
                val arr = line.split(",")
                (arr(0), arr(1))
            })

        val result = originalRDD.distinct().sortByKey(numPartitions = 1).map(x => {
            val domain = x._1
            val name = x._2
            val kv: KeyValue = new KeyValue(Bytes.toBytes(domain), Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name))
            (new ImmutableBytesWritable(Bytes.toBytes(domain)), kv)
        })

        //        result.saveAsNewAPIHadoopFile("/user/root/hbase/user/hfile", classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], conf)
        result.saveAsNewAPIHadoopFile("/user/root/hbase/user/hfile", classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], com.rainsoft.util.java.HBaseUtil.getHbaseJobConf)

        val table : HTable = HBaseUtil.getTable("user")

        val bulkLoader = new LoadIncrementalHFiles(HBaseUtil.getConf)

        bulkLoader.doBulkLoad(new Path("hdfs://dn1.hadoop.com:8020/user/root/hbase/user/hfile"), table)


    }

}
