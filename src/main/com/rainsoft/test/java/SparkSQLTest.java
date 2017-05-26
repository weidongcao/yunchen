package com.rainsoft.test.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import java.util.List;

/**
 * Created by Administrator on 2017-04-26.
 */
public class SparkSQLTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("重点小区人群分析");

        JavaSparkContext sc = new JavaSparkContext(conf);

        HiveContext sqlContext = new HiveContext(sc.sc());

        sqlContext.sql("use yuncai");
        List<Row> list = sqlContext.sql("select * from user").javaRDD().collect();
        for (Row row : list) {
            System.out.println(row.get(0) + "\t" + row.get(1) + "\t" + row.get(2) + "\t" + row.get(3));
        }
    }
}
