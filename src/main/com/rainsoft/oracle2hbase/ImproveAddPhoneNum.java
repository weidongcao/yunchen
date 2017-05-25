package com.rainsoft.oracle2hbase;

import com.rainsoft.hbase.hfile.java.RowkeyColumnSecondarySort;
import com.rainsoft.util.java.Constants;
import com.rainsoft.util.java.HBaseUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

/**
 * Created by Administrator on 2017-05-24.
 */
public class ImproveAddPhoneNum {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf()
                .setAppName("高危人群信息表数据从Oracle迁移到HBase")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        HiveContext sqlContext = new HiveContext(sc.sc());

        sqlContext.sql("use yuncai");
        JavaRDD<Row> improveRDD = sqlContext.sql("select id, imsi_code from h_scan_ending_improve").javaRDD();

        JavaPairRDD<RowkeyColumnSecondarySort, String> aaRDD = improveRDD.mapToPair(
                new PairFunction<Row, RowkeyColumnSecondarySort, String>() {
                    @Override
                    public Tuple2<RowkeyColumnSecondarySort, String> call(Row row) throws Exception {
                        String rowkey = row.get(0).toString();
                        RowkeyColumnSecondarySort sort = new RowkeyColumnSecondarySort(rowkey, "PHONE_NUM");
                        return new Tuple2<>(sort, row.get(1).toString());
                    }
                }
        ).sortByKey();
        HBaseUtil.writeData2HBase(aaRDD, "H_SCAN_ENDING_IMPROVE", "ENDING_IMPROVE", Constants.HFILE_TEMP_STORE_PATH + "H_SCAN_ENDING_IMPROVE");
    }
}
