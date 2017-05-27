package com.rainsoft.spark.java;

import com.rainsoft.hbase.hfile.java.RowkeyColumnSecondarySort;
import com.rainsoft.util.java.*;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

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
                .setAppName("key area analysis result by month");

        JavaSparkContext sc = new JavaSparkContext(conf);

        InputStream streamEmphasisAnalysisResultByMohthSql = EmphasisAnalysisResultByMonth.class.getClassLoader().getResourceAsStream("sql/emphasisAnalysisResultByMonth.sql");

        String fileSql = IOUtils.toString(streamEmphasisAnalysisResultByMohthSql);

        IOUtils.closeQuietly(streamEmphasisAnalysisResultByMohthSql);

        Calendar calendar = Calendar.getInstance();
        Date curdate = DateUtils.TIME_FORMAT.parse(args[0]);
        calendar.setTime(curdate);

        calendar.add(Calendar.HOUR, -1);

        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH) + 1;

        String sql = fileSql.replace("${year}", year + "");

        sql = sql.replace("${month}", month + "");

        HiveContext sqlContext = new HiveContext(sc.sc());
        sqlContext.sql("use yuncai");
        DataFrame originalDF = sqlContext.sql(sql);

        String tableName = TableConstant.HBASE_TABLE_H_EMPHASIS_ANALYSIS_RESULT;
        String hdfsTempPath = Constants.HFILE_TEMP_STORE_PATH + tableName;
        String[] columns = FieldConstant.HBASE_FIELD_MAP.get(tableName);

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
                        String rowkey = "month_" + statDate + "_" + serviceCode + "_" + uuid.substring(0, 16);
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
        HBaseUtil.writeData2HBase(hbasePairRDD, tableName, TableConstant.HBASE_CF_FIELD, hdfsTempPath);
    }
}
