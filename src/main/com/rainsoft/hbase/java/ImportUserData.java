package com.rainsoft.hbase.java;

import com.rainsoft.util.java.DateUtils;
import com.rainsoft.util.java.HBaseUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Date;
import java.util.UUID;

/**
 * Created by Administrator on 2017-05-16.
 */
public class ImportUserData {
    public static void main(String[] args) throws Exception {
        System.setProperty("user.name", "root");
        SparkConf conf = new SparkConf()
                .setAppName("import User data into HBase")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> originalRDD = sc.textFile("file:///D:\\0WorkSpace\\Develop\\data\\bcp\\http");

        JavaPairRDD<String, String> pairRDD = originalRDD.mapToPair(
                new PairFunction<String, String, String>() {
                    @Override
                    public Tuple2<String, String> call(String s) throws Exception {
                        String[] arr = s.split(",");
                        return new Tuple2<>(arr[0], arr[1]);
                    }
                }
        ).sortByKey();
        JavaPairRDD<ImmutableBytesWritable, KeyValue> hfileRDD = pairRDD.mapToPair(
                new PairFunction<Tuple2<String, String>, ImmutableBytesWritable, KeyValue>() {
                    @Override
                    public Tuple2<ImmutableBytesWritable, KeyValue> call(Tuple2<String, String> tuple) throws Exception {
                        String rowkey = tuple._1();
                        String value = tuple._2();
                        KeyValue kv = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(value));
                        ImmutableBytesWritable im = new ImmutableBytesWritable(Bytes.toBytes(rowkey));
                        return new Tuple2<>(im, kv);
                    }
                }
        );
        hfileRDD.saveAsNewAPIHadoopFile("hdfs://dn1.hadoop.com:8020/user/root/hbase/user/hfile", ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, HBaseUtil.getConf());

        HTable table = HBaseUtil.getTable("user");
        LoadIncrementalHFiles bulkLoader = new LoadIncrementalHFiles(HBaseUtil.getConf());

        bulkLoader.doBulkLoad(new Path("hdfs://dn1.hadoop.com:8020/user/root/hbase/user/hfile"), table);
    }
}
