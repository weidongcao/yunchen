package com.rainsoft.spark.java;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Administrator on 2017-04-14.
 */
public class AccumulatorTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Accumulator Test")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        HiveContext hiveContext = new HiveContext(sc.sc());

        JavaRDD<Row> hiveRDD = hiveContext.sql("use yuncai; select * from h_persion_type").javaRDD();

        hiveRDD.foreach(
                new VoidFunction<Row>() {
                    @Override
                    public void call(Row row) throws Exception {
                        System.out.println(row.getString(0));
                    }
                }
        );
    }
}
