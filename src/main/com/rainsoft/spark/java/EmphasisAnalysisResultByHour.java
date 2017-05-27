package com.rainsoft.spark.java;

import com.rainsoft.hbase.hfile.java.RowkeyColumnSecondarySort;
import com.rainsoft.util.java.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.io.InputStream;
import java.util.*;

/**
 * 重点区域人群分析
 * 按小时进行统计
 */
public class EmphasisAnalysisResultByHour {
    //时间时间(一个小时)
    public static long step_length = 3600L;
    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf()
                .setAppName("key area analysis result by hour");

        JavaSparkContext sc = new JavaSparkContext(conf);

        HiveContext sqlContext = new HiveContext(sc.sc());

        //读取Sql文件夹
        InputStream streamEmphasisAnalysisResultSql = EmphasisAnalysisResultByHour.class.getClassLoader().getResourceAsStream("sql/emphasisAnalysisResultByHour.sql");

        String fileSql = IOUtils.toString(streamEmphasisAnalysisResultSql);

        IOUtils.closeQuietly(streamEmphasisAnalysisResultSql);

        //替换SQL文件模板参数
        Calendar calendar = Calendar.getInstance();

        Date curdate = DateUtils.TIME_FORMAT.parse(args[0]);
        calendar.setTime(curdate);

        calendar.add(Calendar.MINUTE, -1);

        String ds = DateUtils.DATE_FORMAT.format(calendar.getTime());
        String hr = NumberUtils.getFormatInt(2, 2, calendar.get(Calendar.HOUR_OF_DAY));

        long curTimestamp = DateUtils.HOUR_FORMAT.parse(ds + " " + hr).getTime();

        String sql = fileSql.replace("${ds}", ds);

        sql = sql.replace("${hr}", hr);
        sql = sql.replace("${cur_timestamp}", curTimestamp + "");
        sql = sql.replace("${step_length}", step_length + "");
        sql = sql.replace("${tablename}", "buffer_emphasis_analysis");

        System.out.println(sql);
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
        //切换数据库
        sqlContext.sql("use yuncai");
        DataFrame originalDF = sqlContext.sql(sql);

        //重点区域分析结果表表名
        String tablename = TableConstant.HBASE_TABLE_H_EMPHASIS_ANALYSIS_RESULT;
        //HDFS临时存储目录
        String hdfsTempPath = Constants.HFILE_TEMP_STORE_PATH + tablename;
        //重点区域分析结果表字段
        String[] columns = FieldConstant.HBASE_FIELD_MAP.get(tablename);

        //对RowKey,字段及值进行处理并进行二次排序
        JavaPairRDD<RowkeyColumnSecondarySort, String> hbasePairRDD = originalDF.javaRDD().flatMapToPair(
                new PairFlatMapFunction<Row, RowkeyColumnSecondarySort, String>() {
                    @Override
                    public Iterable<Tuple2<RowkeyColumnSecondarySort, String>> call(Row row) throws Exception {
                        List<Tuple2<RowkeyColumnSecondarySort, String>> list = new ArrayList<>();
                        String serviceCode = row.getString(1);
                        Date temp = DateUtils.HOUR_FORMAT.parse(row.getString(2));
                        String statDate = DateUtils.STEMP_FORMAT.format(temp);
                        String uuid = UUID.randomUUID().toString().replace("-", "");
                        String rowkey = "hour_" +  statDate + "_" + serviceCode + "_" + uuid.substring(0, 16);
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
        HBaseUtil.writeData2HBase(hbasePairRDD, tablename, TableConstant.HBASE_CF_FIELD, hdfsTempPath);
    }
}

