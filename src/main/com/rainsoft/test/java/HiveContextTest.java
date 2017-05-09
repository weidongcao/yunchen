package com.rainsoft.test.java;

import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by Administrator on 2017-04-25.
 */
public class HiveContextTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("测试HiveContext")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        HiveContext hiveContext = new HiveContext(sc.sc());

//        hiveContext.sql("select * from yuncai.user");
        sqlContext.sql("select * from yuncai.user");

    }
}
