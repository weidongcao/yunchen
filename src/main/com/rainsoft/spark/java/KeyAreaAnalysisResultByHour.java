package com.rainsoft.spark.java;

import com.rainsoft.hbase.hfile.java.RowkeyColumnSecondarySort;
import com.rainsoft.util.java.DateUtils;
import com.rainsoft.util.java.FieldConstant;
import com.rainsoft.util.java.HBaseUtil;
import com.rainsoft.util.java.NumberUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.*;

/**
 * 重点区域人群分析
 * 按小时进行统计
 */
public class KeyAreaAnalysisResultByHour {
    public static long step_length = 3600L;
    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf()
                .setAppName("key area analysis result by hour");

        JavaSparkContext sc = new JavaSparkContext(conf);

        HiveContext sqlContext = new HiveContext(sc.sc());
        sqlContext.sql("use yuncai");

        String fileSql = FileUtils.readFileToString(new File("emphasisAnalysisResult.sql"));

        Calendar calendar = Calendar.getInstance();

        Date curdate = DateUtils.TIME_FORMAT.parse(args[0]);
        calendar.setTime(curdate);

        calendar.add(Calendar.HOUR, -1);

        String ds = DateUtils.DATE_FORMAT.format(curdate);
        String hr = NumberUtils.getFormatInt(2, 2, calendar.get(Calendar.HOUR));

        long curTimestamp = DateUtils.HOUR_FORMAT.parse(ds + " " + hr).getTime();

        String sql = fileSql.replace("${ds}", ds);

        sql = sql.replace("${hr}", hr);
        sql = sql.replace("${cur_timestamp}", curTimestamp + "");
        sql = sql.replace("${step_length}", step_length + "");
        sql = sql.replace("${tablename}", "buffer_emphasis_analysis");

        writeEmpahsisAnalysisResult2HBase(sqlContext, sql);
    }

    /**
     * 将重点区域分析结果写HBase
     * 5分钟统计一次的时候也会用到
     * @param sqlContext
     * @param sql
     * @throws Exception
     */
    public static void writeEmpahsisAnalysisResult2HBase(HiveContext sqlContext, String sql) throws Exception {
        DataFrame originalDF = sqlContext.sql(sql);

        String[] columns = FieldConstant.HBASE_FIELD_MAP.get("emphasis_analysis_result");

        JavaPairRDD<RowkeyColumnSecondarySort, String> hbasePairRDD = originalDF.javaRDD().flatMapToPair(
                new PairFlatMapFunction<Row, RowkeyColumnSecondarySort, String>() {
                    @Override
                    public Iterable<Tuple2<RowkeyColumnSecondarySort, String>> call(Row row) throws Exception {
                        List<Tuple2<RowkeyColumnSecondarySort, String>> list = new ArrayList<>();
                        String serviceCode = row.getString(1);
                        Date temp = DateUtils.HOUR_FORMAT.parse(row.getString(2));
                        String statDate = DateUtils.STEMP_FORMAT.format(temp);
                        String uuid = UUID.randomUUID().toString().replace("-", "");
                        String rowkey = statDate + "_" + serviceCode + "_" + uuid.substring(0, 16);
                        for (int i = 0; i < columns.length; i++) {
                            list.add(new Tuple2<>(new RowkeyColumnSecondarySort(rowkey, columns[i]), row.get(i).toString()));
                        }
                        return list;
                    }
                }
        ).sortByKey();

        //写入HBase
        HBaseUtil.writeData2HBase(hbasePairRDD, "h_emphasis_analysis_result", "field", "/user/root/hbase/table/h_emphasis_analysis_result");
    }
}

