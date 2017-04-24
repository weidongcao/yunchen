package com.rainsoft.spark.java;

import com.rainsoft.util.java.HBaseUtil;
import com.rainsoft.util.java.IMSIUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

/**
 * 功能说明：通过IMSI号生成IMSI号的手机号前7位以及地区和运营商
 * Created by Administrator on 2017-04-20.
 */
public class CreateIMSIKeyArea {

    public static void main(String[] args) throws IOException {
        //Spark配置对象
        SparkConf conf = new SparkConf()
                .setAppName("通过IMSI号生成IMSI号的手机号前7位以及地区和运营商")
                .setMaster("local");

        //创建SparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        //创建sqlContext
        SQLContext sqlContext = new SQLContext(sc);

        //读取IMSI号
        JavaRDD<String> imsiRDD = sc.textFile("file:///D:\\0WorkSpace\\Develop\\data\\distinct_imsi_20170308_to_20170419");

        //通过IMSI号生成手机号前7位及手机号归属地、运营商等
        DataFrame infoDF = IMSIUtils.createIMSIKeyArea(sc, sqlContext, imsiRDD, "local_window");

        //将DataFrame转化为javaRDD
        JavaRDD<Row> infoRDD = infoDF.javaRDD();

        //生成要写入HBase的数据
        JavaPairRDD<ImmutableBytesWritable, Put> hbaseIMSIRDD = infoRDD.mapToPair(
                new PairFunction<Row, ImmutableBytesWritable, Put>() {
                    @Override
                    public Tuple2<ImmutableBytesWritable, Put> call(Row row) throws Exception {
                        //以UUID作为RowKey
                        String uuid = UUID.randomUUID().toString().replace("-", "");
                        //Hbase一条写入的信息
                        Put put = new Put(Bytes.toBytes(uuid));

                        String imsi = row.getString(0);
                        String phone7 = row.getString(1);
                        String areaName = row.getString(2);
                        String areaCode = row.getString(3);
                        String phoneType = row.getString(4);
                        String region = row.getString(5);

                        put.addColumn(Bytes.toBytes("IMSI_KEY_AREA"), Bytes.toBytes("imsi_num"), Bytes.toBytes(imsi));

                        if (phone7 != null) {
                            put.addColumn(Bytes.toBytes("IMSI_KEY_AREA"), Bytes.toBytes("phone_num"), Bytes.toBytes(phone7));
                        }

                        if (areaName != null) {
                            put.addColumn(Bytes.toBytes("IMSI_KEY_AREA"), Bytes.toBytes("area_name"), Bytes.toBytes(areaName));
                        }

                        if (areaCode != null) {
                            put.addColumn(Bytes.toBytes("IMSI_KEY_AREA"), Bytes.toBytes("area_code"), Bytes.toBytes(areaCode));
                        }

                        if (phoneType != null) {
                            put.addColumn(Bytes.toBytes("IMSI_KEY_AREA"), Bytes.toBytes("phone_type"), Bytes.toBytes(phoneType));
                        }

                        if (region != null) {
                            put.addColumn(Bytes.toBytes("IMSI_KEY_AREA"), Bytes.toBytes("region"), Bytes.toBytes(region));
                        }

                        return new Tuple2<>(new ImmutableBytesWritable(), put);
                    }
                }
        );

        //创建HBase的job
        JobConf jobConf = HBaseUtil.getHbaseJobConf();
        //设置表名
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, "H_SYS_IMSI_KEY_AREA");
        //写入HBase
        hbaseIMSIRDD.saveAsHadoopDataset(jobConf);
    }

}
