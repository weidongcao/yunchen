package com.rainsoft.spark.java;

import com.rainsoft.hbase.hfile.java.RowkeyColumnSecondarySort;
import com.rainsoft.util.java.DateUtils;
import com.rainsoft.util.java.FieldConstant;
import com.rainsoft.util.java.HBaseUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.io.File;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Administrator on 2017-05-16.
 */
public class EmphasisAnalysisResultByMonth {
    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf()
                .setAppName("key area analysis result by month")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        InputStream streamEmphasisAnalysisResultByMohthSql = EmphasisAnalysisResultByMonth.class.getClassLoader().getResourceAsStream("sql/emphasisAnalysisResultByMohth.sql");

        String fileSql = IOUtils.toString(streamEmphasisAnalysisResultByMohthSql);

        IOUtils.closeQuietly(streamEmphasisAnalysisResultByMohthSql);

        args[0] = "2017-04-30 00:00:00";
        Calendar calendar = Calendar.getInstance();
        System.out.println(args[0]);
        Date curdate = DateUtils.TIME_FORMAT.parse(args[0]);
        System.out.println(DateUtils.TIME_FORMAT.format(curdate));
        calendar.setTime(curdate);
        System.out.println(DateUtils.TIME_FORMAT.format(calendar.getTime()));

        calendar.add(Calendar.HOUR, -1);
        System.out.println(DateUtils.TIME_FORMAT.format(calendar.getTime()));


        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH) + 1;

        String sql = fileSql.replace("${year}", year + "");

        sql = sql.replace("${month}", month + "");

        HiveContext sqlContext = new HiveContext(sc.sc());
        sqlContext.sql("use yuncai");
        DataFrame originalDF = sqlContext.sql(sql);

        String[] columns = FieldConstant.HBASE_FIELD_MAP.get("emphasis_analysis_result");

        DateFormat monthFormat = new SimpleDateFormat("yyyy-MM");
        JavaPairRDD<RowkeyColumnSecondarySort, String> hbasePairRDD = originalDF.javaRDD().flatMapToPair(
                new PairFlatMapFunction<Row, RowkeyColumnSecondarySort, String>() {
                    @Override
                    public Iterable<Tuple2<RowkeyColumnSecondarySort, String>> call(Row row) throws Exception {
                        List<Tuple2<RowkeyColumnSecondarySort, String>> list = new ArrayList<>();
                        String serviceCode = row.getString(1);
                        Date temp = monthFormat.parse(row.getString(2));
                        String statDate = DateUtils.STEMP_FORMAT.format(temp);
                        String uuid = UUID.randomUUID().toString().replace("-", "");
                        String rowkey = statDate + "_" + serviceCode + "_" + uuid.substring(0, 16);
                        for (int i = 0; i < columns.length; i++) {
                            if (null != row.get(i)) {
                                list.add(new Tuple2<>(new RowkeyColumnSecondarySort(rowkey, columns[i]), row.get(i).toString()));
                            }
                        }
                        return list;
                    }
                }
        ).sortByKey();

        //写入HBase
        HBaseUtil.writeData2HBase(hbasePairRDD, "h_emphasis_analysis_result", "field", "/user/root/hbase/table/h_emphasis_analysis_result");


    }
}
