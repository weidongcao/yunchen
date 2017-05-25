package com.rainsoft.hbase.hfile.java;

import com.rainsoft.util.java.FieldConstant;
import com.rainsoft.util.java.HBaseUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import scala.Tuple2;

import java.util.*;

/**
 * Created by Administrator on 2017-05-16.
 */
public class CreateUserHFile {
    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf()
                .setAppName("create user of hbase table hfile")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> originalRDD = sc.textFile("file:///D:\\0WorkSpace\\Develop\\data\\user_data.txt");

        //表的字段名
        String[] columns = FieldConstant.HBASE_FIELD_MAP.get("user");

        //生成的HFile的临时保存路径
        String hfilePath = "hdfs://dn1.hadoop.com:8020/user/root/hbase/user/hfile7";

        Date date = new Date();

        JavaPairRDD<RowkeyColumnSecondarySort, String> fieldRDD = originalRDD.flatMapToPair(
                new PairFlatMapFunction<String, RowkeyColumnSecondarySort, String>() {
                    @Override
                    public Iterable<Tuple2<RowkeyColumnSecondarySort, String>> call(String s) throws Exception {
                        //要返回的List
                        List<Tuple2<RowkeyColumnSecondarySort, String>> list = new ArrayList<>();
                        //各个字段的值
                        String[] arr = s.split(",");
                        //生成UUID
                        String uuid = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
                        String rowkey = arr[0] + "_" + arr[1] + "_" + uuid;

                        for (int i = 0; i < columns.length; i++) {
                            //RowkeyColumnSecondarySort封装了HBase一条数据的rowkey和字段名,通过此封闭类对其进行二次排序,否则生成HFile失败
                            list.add(new Tuple2<>(new RowkeyColumnSecondarySort(rowkey, columns[i]), arr[i]));
                        }
                        return list;
                    }
                }
        ).sortByKey();
        HBaseUtil.writeData2HBase(fieldRDD, "user", "info", hfilePath);
    }
}
