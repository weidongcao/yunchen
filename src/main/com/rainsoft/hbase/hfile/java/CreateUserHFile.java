package com.rainsoft.hbase.hfile.java;

import com.rainsoft.util.java.FieldConstant;
import com.rainsoft.util.java.HBaseUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2017-05-16.
 */
public class CreateUserHFile {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("create user of hbase table hfile")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> originalRDD = sc.textFile("file:///D:\\0WorkSpace\\Develop\\data\\user_data.txt");

        String[] userFields = FieldConstant.HBASE_FIELD_MAP.get("user");

        JavaPairRDD<ImmutableBytesWritable, KeyValue> hbaseRDD = originalRDD.mapToPair(
                new PairFunction<String, ImmutableBytesWritable, KeyValue>() {
                    @Override
                    public Tuple2<ImmutableBytesWritable, KeyValue> call(String s) throws Exception {
                        String[] arr = s.split(",");

                        ImmutableBytesWritable im = new ImmutableBytesWritable(Bytes.toBytes(arr[0]));
                        for (int i = 0; i < userFields.length; i++) {
                            KeyValue kv = new KeyValue(Bytes.toBytes("info"), Bytes.toBytes(userFields[i]), Bytes.toBytes(arr[i + 1]));

                        }
                        return null;
                    }
                }
        );



    }
}
