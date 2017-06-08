package com.rainsoft.test.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import java.util.List;

/**
 * Created by Administrator on 2017-04-26.
 */
public class SparkSQLTest {
    public static void main(String[] args) {
        String aaa = "[\"吸毒/贩毒\",\"盗窃/抢劫\"]";
        Row row = RowFactory.create(aaa);
        System.out.println(row.mkString(","));
    }
}
