package com.rainsoft.test.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
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
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

/**
 * Created by Administrator on 2017-04-19.
 */
public class ZookeeperTest {
    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf()
                .setAppName("测试Zookeeper是否正常")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        String[] arr = new String[]{"aaa,bbb,ccc,ddd"};

        JavaRDD<String> oRDD = sc.parallelize(Arrays.asList(arr));

        JavaRDD<Row> rowRDD = oRDD.map(
                (Function<String, Row>) s -> {
                    String[] arr1 = s.split(",");
                    return RowFactory.create(arr1[0], arr1[1], arr1[2], arr1[3]);
                }
        );

        JavaPairRDD<ImmutableBytesWritable, Put> hbaseRDD = rowRDD.mapToPair(
                (PairFunction<Row, ImmutableBytesWritable, Put>) row -> {
                    //HBase数据的rowkey以UUID的格式生成
                    String uuid = UUID.randomUUID().toString().replace("-", "");
                    Put put = new Put(Bytes.toBytes(uuid));
                    String TEMP_CF_PEOPLE_ANALYSIS = "content";
                    //小区人员IMSI号
                    put.addColumn(Bytes.toBytes(TEMP_CF_PEOPLE_ANALYSIS), Bytes.toBytes("aaa"), Bytes.toBytes(row.getString(0)));
                    //小区名
                    put.addColumn(Bytes.toBytes(TEMP_CF_PEOPLE_ANALYSIS), Bytes.toBytes("bbb"), Bytes.toBytes(row.getString(1)));
                    //小区ID
                    put.addColumn(Bytes.toBytes(TEMP_CF_PEOPLE_ANALYSIS), Bytes.toBytes("ccc"), Bytes.toBytes(row.getString(2)));
                    //信息采集设备ID
                    put.addColumn(Bytes.toBytes(TEMP_CF_PEOPLE_ANALYSIS), Bytes.toBytes("ddd"), Bytes.toBytes(row.getString(3)));

                    //返回HBase格式的数据
                    return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
                }
        );

        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource("hbase-site.xml");

        Connection conn = ConnectionFactory.createConnection(configuration);

        JobConf jobConf = new JobConf(configuration);
        jobConf.setOutputFormat(TableOutputFormat.class);

        jobConf.set(TableOutputFormat.OUTPUT_TABLE, "test_chat");

        hbaseRDD.saveAsHadoopDataset(jobConf);
    }
}
