package com.rainsoft.spark.scala

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import com.rainsoft.util.java.IMSIUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 实现思路：
  *
  * 	1. 获取BCP文件里的IMSI号
  * 	2. 根据IMSI号生成手机号前7位
  * 		1. 根据IMSI.java里提供的规则
  *
  * 	3. 根据手机号前7位获取手机运营商及归属地
  * 		1. 根据h_phone表获取
  *
  * 	4. 将IMSI号、手机号前7位、手机运营商、归属地插入Oracle数据库的sys_phone_to_area表中
  * 	5. 将BCP文件导入HBase数据库的H_SCAN_ENDING_IMPROVE表中
  * 	6. 删除BCP文件
  * 	7. 根据分析结果进行各种统计。。。。。。
  *
  * @author Cao Weidong
  *         date 2017-03-23.
  */
object checkIMSI extends Serializable {

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //Hbase表
    val TABLE_H_SCAN_ENDING_IMPROVE = "H_SCAN_ENDING_IMPROVE"
    val TABLE_H_SYS_IMSI_KEY_AREA = "H_SYS_IMSI_KEY_AREA"
    //列族
    val CF_H_SCAN_ENDING_IMPROVE = "ENDING_IMPROVE"
    val CF_H_SYS_IMSI_KEY_AREA = "IMSI_KEY_AREA"


    //时间格式
    val dateFormater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    def main(args: Array[String]): Unit = {

        //BCP文件路径
        val INPUT = args(0).toString

        //HBase属性配置
        val hbaseConf = HBaseConfiguration.create()
        //HBase的Zookeeper端口
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
        //设置zookeeper集群
        hbaseConf.set("hbase.zookeeper.quorum", "nn1,nn2,dn1,dn2,dn3")
        //设置HMatser
        hbaseConf.set("hbase.zookeeper.master", "dn3:60000")


        // ======Save RDD to HBase========
        // step 1: JobConf setup
        val jobConf = new JobConf(hbaseConf, this.getClass)
        jobConf.setOutputFormat(classOf[TableOutputFormat])


        //创建SparkConf
        val conf = new SparkConf()
            .setAppName("checkIMSI")

        //创建SparkContext
        val sc = new SparkContext(conf)
        //创建HiveContext
        val hiveContext = new HiveContext(sc)

        //读取数据
        val originalRDD = sc.textFile("file://" + INPUT)

        //将BCP文件进行行及字段切分,字段分隔符为|#|;行分隔符为|$|
        val fieldRDD = originalRDD.map(line => {
            //行分隔
            val newLine = line.split("\\|$\\|")(0)
            //字段分隔
            val fields = newLine.split("\\|#\\|")
            fields
        })

        //将数据持久化到内存
        fieldRDD.persist()

        /**
          * 根据IMSI号生成手机号前7位
          */
        val infoRDD = fieldRDD.map(fields => {
            //IMSI号
            val IMSICode = fields(1)
            //根据IMSI号生成手机号前7位
            val mobile = IMSIUtils.getMobileAll(IMSICode)

            //返回<手机号, IMSI号>KeyValue对
            (mobile, IMSICode)
        })

        /**
          * 根据手机号前7位获取手机运营商及归属地
          */
        //指定Hive数据库
        hiveContext.sql("use yuncai")
        //指定数据库表(H_SYS_PHONE_TO_AREA),及要查询的字段(PHONE_NUM, AREA_NAME, AREA_CODE, PHONE_TYPE, REGION)
        val phoneRDD = hiveContext.sql("select PHONE_NUM, AREA_NAME, AREA_CODE, PHONE_TYPE, REGION from H_SYS_PHONE_TO_AREA").rdd
            .map(row => (
                //将查询出来出来的结果转为<key, value>对，Key为手机号前7位，value为剩余字段的数组
                row(0).toString, //Key：手机号前7位
                Array(//Value：数组
                    row(1).toString, //字段：AREA_NAME
                    row(2).toString, //字段：AREA_CODE
                    row(3).toString, //字段：PHONE_TYPE
                    row(4).toString //字段：REGION
                )))

        /**
          * IMSI生成的手机号前7位与手机号信息表Join,取得些手机号归属地及其他信息
          * 最终的结果为即将插入HBase表的数据结果集的数据格式
          * 数据格式：[IMSI_NUM, PHONE_NUM, AREA_NAME, AREA_CODE, PHONE_TYPE, REGION]
          * 字段含义：[IMSI号,  手机号前7位,    归属地, 归属地代码, 手机运营商,   电话区号]
          */
        val phoneInfoRDD = infoRDD.join(phoneRDD) //IMSI生成的手机号与手机号信息表Join
            .map(tuple => {
            Array(
                tuple._2._1, //字段：IMSI_NUM
                tuple._1, //字段：PHONE_NUM
                tuple._2._2(0), //字段：AREA_NAME
                tuple._2._2(1), //字段：AREA_CODE
                tuple._2._2(2), //字段：PHONE_TYPE
                tuple._2._2(3) //字段：REGION
            )
        })

        /**
          * 将IMSI号、手机号前7位、手机运营商、归属地插入Oracle数据库的sys_phone_to_area表中
          *
          */
        val phoneHBaseRDD = phoneInfoRDD.map(arr => {
            //生成rowkey,以省加IMSI号的形式
            val rowKey = arr(2).split("省")(0) + arr(0)
            val array = Array(
                ("IMSI_NUM", arr(0)),
                ("PHONE_NUM", arr(1)),
                ("AREA_NAME", arr(2)),
                ("AREA_CODE", arr(3)),
                ("PHONE_TYPE", arr(4)),
                ("REGION", arr(5)))
            convertHBaseDataFormat(rowKey, CF_H_SYS_IMSI_KEY_AREA, array)
        })

        //指定要插入HBase的表名
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, TABLE_H_SYS_IMSI_KEY_AREA)

        //数据插入HBase
        phoneHBaseRDD.saveAsHadoopDataset(jobConf)

        //将BCP文件导入HBase数据库的H_SCAN_ENDING_IMPROVE表中
        try {
            println("开始索引时间：>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + dateFormater.format(new Date) + "<<<<<<<<<<<<<<<<<<<<<<<<")

            val hbaseRDD = fieldRDD.map(fields => {
                val rowKey = UUID.randomUUID().toString.replace("-", "")
                val importTime = dateFormat.format(new Date())
                val array = Array(
                        ("ENDING_MAC", fields(0)),
                        ("IMSI_CODE", fields(1)),
                        ("CAPTURE_TIME", fields(2)),
                        ("OPERATOR_TYPE", fields(3)),
                        ("SN_CODE", fields(4)),
                        ("LONGITUDE", fields(5)),
                        ("LATITUDE", fields(6)),
                        ("FIRST_TIME", fields(7)),
                        ("LAST_TIME", fields(8)),
                        ("DIST", fields(9)),
                        ("IMPORT_TIME", importTime)
                )
                convertHBaseDataFormat(rowKey, CF_H_SCAN_ENDING_IMPROVE, array)
            })

            //指定要插入HBase的表名
            jobConf.set(TableOutputFormat.OUTPUT_TABLE, TABLE_H_SCAN_ENDING_IMPROVE)
            //插入HBase
            hbaseRDD.saveAsHadoopDataset(jobConf)

            println("结束索引时间：>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + dateFormater.format(new Date))

        } catch {
            case e: Exception => println(e.printStackTrace()); System.exit(-1)
            //      case unknown => println("Unknown exception " + unknown); System.exit(-1)
        } finally {
            //关闭SparkContext上下文
            sc.stop()
            //清空文件夹
            deleteFolder(INPUT)
        }

        //根据分析结果进行各种统计。。。。。。
    }

    /**
      * 删除指定目录下已经处理过的BCP文件
      *
      * @param dir
      */
    def deleteFolder(dir: String) {
        //创建文件对象
        val delfolder = new File(dir);

        //判断此文件对象是否存在且为目录
        if ((delfolder.exists()) && (delfolder.isDirectory)) {
            //获取目录下所有的文件列表，包括目录和文件
            val oldFile = delfolder.listFiles();

            try {
                //遍历所有文件
                for (i <- 0 to oldFile.length - 1) {
                    //子文件
                    val childFile = oldFile(i)

                    //判断子文件是否存在
                    if (childFile.exists()) {

                        //判断子文件是文件还是目录
                        if (childFile.isDirectory) {
                            //目录的话递归删除所有
                            deleteFolder(dir + "/" + childFile.getName)
                        } else if (childFile.isFile) {
                            //文件直接删除
                            childFile.delete()
                        }
                    }
                }
                println("清空文件夹操作成功!")
            }
            catch {
                case e: Exception => println(e.printStackTrace());
                    println("清空文件夹操作出错!")
                    System.exit(-1)
            }
        }

    }

    def convertHBaseDataFormat(rowKey: String, cf: String, arr: Array[(String, String)]): (ImmutableBytesWritable, Put) = {
        val put = new Put(Bytes.toBytes(rowKey))
        for (tuple <- arr) {
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(tuple._1), Bytes.toBytes(tuple._2))
        }

        (new ImmutableBytesWritable, put)
    }
}
