package com.rainsoft.hbase.java;

import com.rainsoft.hbase.hfile.java.RowkeyColumnSecondarySort;
import com.rainsoft.util.java.FieldConstant;
import com.rainsoft.util.java.HBaseUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2017-05-19.
 */
public class ImportImproveData {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf()
                .setAppName(ImportImproveData.class.getSimpleName())
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> aaaRDD = sc.textFile("file:///D:\\0WorkSpace\\Develop\\data\\improve.txt");

        String[] cols = FieldConstant.HBASE_FIELD_MAP.get("h_scan_ending_improve");

        JavaPairRDD<RowkeyColumnSecondarySort, String> hfileRDD = aaaRDD.flatMapToPair(
                new PairFlatMapFunction<String, RowkeyColumnSecondarySort, String>() {
                    @Override
                    public Iterable<Tuple2<RowkeyColumnSecondarySort, String>> call(String s) throws Exception {
                        List<Tuple2<RowkeyColumnSecondarySort, String>> list = new ArrayList<Tuple2<RowkeyColumnSecondarySort, String>>();
                        String[] arr = s.split("\t");
                        String rowKey = arr[0];
                        for (int i = 0; i < cols.length; i++) {
                            list.add(new Tuple2<>(new RowkeyColumnSecondarySort(rowKey, cols[i]), arr[i + 1]));
                        }
                        return list;
                    }
                }
        ).sortByKey();

        String hfilePath = "hdfs://dn1.hadoop.com:8020/user/root/hbase/table/h_scan_ending_improve/hfile";
        HBaseUtil.writeData2HBase(hfileRDD, "H_SCAN_ENDING_IMPROVE", "ENDING_IMPROVE", hfilePath);
    }
}
