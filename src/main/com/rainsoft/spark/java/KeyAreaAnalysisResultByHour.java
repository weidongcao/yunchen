package com.rainsoft.spark.java;

import com.rainsoft.util.java.DateUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;

/**
 * 重点区域人群分析
 * 按小时进行统计
 */
public class KeyAreaAnalysisResultByHour {
    public static void main(String[] args) throws IOException {

        SparkConf conf = new SparkConf()
                .setMaster("key area analysis result by hour")
                .setMaster("yarn-client");

        JavaSparkContext sc = new JavaSparkContext(conf);

        HiveContext sqlContext = new HiveContext(sc.sc());

        String fileSql = FileUtils.readFileToString(new File("D:\\0WorkSpace\\JetBrains\\yunchen\\src\\main\\resource\\sql\\emphasisAnalysisResult.sql"));

        Calendar calendar = Calendar.getInstance();

        calendar.add(Calendar.HOUR, -1);

        String ds = DateUtils.DATE_FORMAT.format(calendar.getTime());
        String hr = calendar.get(Calendar.HOUR) + "";

        String sql = fileSql.replace("${ds}", ds);

        sql = sql.replace("${hr}", hr);

        DataFrame originalDF = sqlContext.sql(sql);

        JavaPairRDD<ImmutableBytesWritable, Put> hbasePairRDD = originalDF.javaRDD().mapToPair(
                new PairFunction<Row, ImmutableBytesWritable, Put>() {
                    @Override
                    public Tuple2<ImmutableBytesWritable, Put> call(Row row) throws Exception {
                        String serviceCode = row.getString(1);
                        long statDate = DateUtils.HOUR_FORMAT.parse(row.getString(2)).getTime();
                        String uuid = UUID.randomUUID().toString().replace("-", "");
                        String rowkey = serviceCode + "_" + statDate + "_" + uuid.substring(0, 9);


                        return null;
                    }
                }
        );
    }
}

