package com.rainsoft.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

/**
 * Created by Administrator on 2017-04-07.
 */
public class BcpChat {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("bcp transform")
                .setMaster("local");

        SparkContext sc = new SparkContext(conf);


    }
}
