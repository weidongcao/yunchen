package com.rainsoft.spark.java;

import com.rainsoft.hbase.hfile.java.RowkeyColumnSecondarySort;
import com.rainsoft.util.java.*;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.io.InputStream;
import java.util.*;

/**
 * 按天统计重点区域人人群分类
 * Created by CaoWeiDong on 2017-05-16.
 */
public class EmphasisAnalysisResultByDay {
    public static void main(String[] args) throws Exception {


        //读取SQL模板内容
        InputStream streamEmphasisAnalysisResultByMohthSql = EmphasisAnalysisResultByDay.class.getClassLoader().getResourceAsStream("sql/emphasisAnalysisResultByDay.sql");

        String fileSql = IOUtils.toString(streamEmphasisAnalysisResultByMohthSql);

        IOUtils.closeQuietly(streamEmphasisAnalysisResultByMohthSql);

        //替换sql模板参数
        Calendar calendar = Calendar.getInstance();
        Date curdate = DateUtils.TIME_FORMAT.parse(args[0]);
        calendar.setTime(curdate);

        calendar.add(Calendar.HOUR, -1);

        String ds = DateUtils.DATE_FORMAT.format(calendar.getTime());

        String sql = fileSql.replace("${ds}", ds);

        System.out.println(sql);

        SparkConf conf = new SparkConf()
                .setAppName("key area analysis result by day");

        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext sqlContext = new HiveContext(sc.sc());
        sqlContext.sql("use yuncai");
        JavaRDD<Row> originalRDD = sqlContext.sql(sql).javaRDD();

        /*JavaRDD<Row> originalRDD = sc.textFile("file:///D:\\0WorkSpace\\Develop\\data\\emphasisDay.txt")
                .map(
                        new Function<String, Row>() {
                            @Override
                            public Row call(String v1) throws Exception {
                                String[] fields = v1.split("\t");
                                return RowFactory.create(fields);
                            }
                        }
                );*/

        //重点区域分析结果表表名
        String tableName = TableConstant.HBASE_TABLE_H_EMPHASIS_ANALYSIS_RESULT;
        //HDFS临时文件存储目录
        String hdfsTempPath = Constants.HFILE_TEMP_STORE_PATH + tableName;
        //重点区域分析结果表字段
        String[] columns = FieldConstant.HBASE_FIELD_MAP.get(tableName);

        JavaPairRDD<RowkeyColumnSecondarySort, String> hbasePairRDD = originalRDD.flatMapToPair(
                new PairFlatMapFunction<Row, RowkeyColumnSecondarySort, String>() {
                    @Override
                    public Iterable<Tuple2<RowkeyColumnSecondarySort, String>> call(Row row) throws Exception {
                        List<Tuple2<RowkeyColumnSecondarySort, String>> list = new ArrayList<>();
                        String serviceCode = row.getString(1);
                        Date temp = DateUtils.DATE_FORMAT.parse(row.getString(2));
                        String statDate = DateUtils.STEMP_FORMAT.format(temp);
                        String uuid = UUID.randomUUID().toString().replace("-", "");
                        String rowkey = "day_" + statDate + "_" + serviceCode + "_" + uuid.substring(0, 16);
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
