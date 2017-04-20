package com.rainsoft.scala.util

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by Administrator on 2017-03-27.
  */
object HBaseUtil {
    def convertHBaseDataFormat(rowKey: String, cf: String, arr: Array[(String, String)]): (ImmutableBytesWritable, Put) = {
        val put = new Put(Bytes.toBytes(rowKey))
        for (tuple <- arr) {
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(tuple._1), Bytes.toBytes(tuple._2))
        }

        (new ImmutableBytesWritable, put)
    }

}
