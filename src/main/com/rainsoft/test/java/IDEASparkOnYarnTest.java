package com.rainsoft.test.java;

import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 2017-04-26.
 */
public class IDEASparkOnYarnTest {

    private static final Pattern SPACE = Pattern.compile("\t");

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "hadoop");

        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("测试Window下在IDEA中向集群提交作业");
        sparkConf.setMaster("yarn-client");
        sparkConf.set("spark.yarn.dist.files", "D:\\0WorkSpace\\JetBrains\\yunchen\\src\\resource\\yarn-site.xml");
        sparkConf.set("spark.yarn.jar", "hdfs://dn1.hadoop.com:8020/user/spark/share/lib/spark-assembly-1.5.0-hadoop2.6.0-cdh5.6.1.jar");

        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        ctx.addJar("D:\\0WorkSpace\\JetBrains\\yunchen\\out\\artifacts\\hiveContext\\yunchen.jar");
        JavaRDD<String> lines = ctx.textFile(args[0], 1);

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) {
                return Arrays.asList(SPACE.split(s));
            }
        });

        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        ctx.stop();
    }
}
