package com.rainsoft.hbase.scala

import java.util.Date

import com.rainsoft.dao.factory.DaoFactory
import com.rainsoft.dao.jdbc.JDBCHelper
import com.rainsoft.manager.ConfManager
import com.rainsoft.util.java.{PropConstants, DateUtils}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017-05-09.
  */
object ImportFtpBcpData2HBase {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("import FTP pcb data into HBase")
            .setMaster("local")

        val sc = new SparkContext(conf)

        val sqlContext = new HiveContext(sc)

        val record = DaoFactory.getImportBcpRecordDao.getRecord("ftp")

        val curDate = new Date()
        if (null != record) {
            val diff = (curDate.getTime - record.getImportTime.getTime) / 1000
            val interval = ConfManager.getInteger(PropConstants.IMPORT_BCP_DATA_TIME_INTERVAL)
            if (diff > interval) {
                var times = diff / interval
                if (diff % interval != 0) {
                    times = times + 1
                }

            }
        }
    }
}
