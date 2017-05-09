package com.rainsoft.hbase.java;

import com.rainsoft.dao.IImportBcpRecordDao;
import com.rainsoft.dao.factory.DaoFactory;
import com.rainsoft.domain.java.ImportBcpRecord;
import com.rainsoft.manager.ConfManager;
import com.rainsoft.util.java.*;
import net.sf.json.JSONArray;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.*;

/**
 * Created by CaoWeidong on 2017-05-09.
 * <p>
 * 将Ftp的BCP数据导入到HBase中
 * 此作业会每隔一段时间执行一次，
 * 执行的时候会检查前一时间段的数据有没有导入成功，
 * 如果没有导入成功的话会先把前一时间段的数据导入成功以后再执行本次作业
 * <p>
 * 导入当前时间段的数据从本地BCP文件所有目录获取数据
 * 导入前一时间段的数据从Oracle读取数据
 * <p>
 * 导入之前从Mysql查询检查是否有之前未导入的数据
 * <p>
 * 导入成功后在Mysql表中记录
 */
public class ImportFtpBcpData2HBase {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("import FTP pcb data into HBase")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        String tablename = ConfManager.getProperty(HBaseConstants.TABLE_H_REG_CONTENT_FTP);
        String cf = ConfManager.getProperty(HBaseConstants.CF_H_REG_CONTENT_FTP);
        int interval = ConfManager.getInteger(PropConstants.IMPORT_BCP_DATA_TIME_INTERVAL);
        String url = ConfManager.getProperty(PropConstants.JDBC_URL);
        Date curDate = new Date();
        JSONArray ftpTableFields = JSONArray.fromObject(ConfManager.getProperty(PropConstants.FIELD_REG_CONTENT_FTP));

        HiveContext sqlContext = new HiveContext(sc.sc());
        IImportBcpRecordDao recordDao = DaoFactory.getImportBcpRecordDao();
        ImportBcpRecord recentlyRecord = recordDao.getRecord("ftp");
        Date lastImportTime = recentlyRecord.getImportTime();

        while (true) {
            long diff = (curDate.getTime() - lastImportTime.getTime()) / 1000;
            if (diff > interval) {
                Date curImportTime = new Date(lastImportTime.getTime() + (interval * 1000));

                String[] conditions = new String[]{"capture_time > '${preDate}'", "capture_time < '${date}'"};

                conditions[0] = conditions[0].replace("${preDate}", DateUtils.TIME_FORMAT.format(lastImportTime));
                conditions[1] = conditions[1].replace("${date}", DateUtils.TIME_FORMAT.format(curImportTime));

                DataFrame oracleOriginalDF = sqlContext.read().jdbc(url, tablename, conditions, JDBCUtils.getJDBCProp());
                JavaPairRDD<ImmutableBytesWritable, Put> hbasePairRDD = oracleOriginalDF.javaRDD().mapToPair(
                        new PairFunction<Row, ImmutableBytesWritable, Put>() {
                            @Override
                            public Tuple2<ImmutableBytesWritable, Put> call(Row row) throws Exception {
                                Put put = HBaseUtil.createHBasePut(row, ftpTableFields, cf);
                                return new Tuple2<>(new ImmutableBytesWritable(), put);
                            }
                        }
                );

                insertHBase(hbasePairRDD, tablename, curImportTime, "ftp");

                lastImportTime = curImportTime;
            } else {
                break;
            }
        }
        //Bcp 文件所在路径
//        String ftpDataPath = "file://" + ConfManager.getProperty(PropConstants.DIR_BCP_RESOURCE) + "/ftp/*ftp_content.bcp";
        String ftpDataPath = "file:///D:\\0WorkSpace\\Develop\\data\\scan_ending_improve_bcp";

        JavaRDD<String> originalRDD = sc.textFile(ftpDataPath);

        System.out.println("开始导入时间：>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + DateUtils.TIME_FORMAT.format(new Date()));

        JavaRDD<Row> ftpRowRDD = originalRDD.map(
                new Function<String, Row>() {
                    @Override
                    public Row call(String str) throws Exception {

                        String[] arr = str.split("\\|#\\|");

                        return RowFactory.create(arr);
                    }
                }
        );

        JavaRDD<Row> filterFtpRDD = ftpRowRDD.filter(
                (Function<Row, Boolean>) v1 -> {
                    if (v1.length() == 24) {
                        return true;
                    } else {
                        return false;
                    }
                }
        );


        JavaPairRDD<ImmutableBytesWritable, Put> hbasePairRDD = filterFtpRDD.mapToPair(
                new PairFunction<Row, ImmutableBytesWritable, Put>() {
                    @Override
                    public Tuple2<ImmutableBytesWritable, Put> call(Row row) throws Exception {
                        Put put = HBaseUtil.createHBasePut(row, ftpTableFields, cf);
                        return new Tuple2<>(new ImmutableBytesWritable(), put);
                    }
                }
        );

        insertHBase(hbasePairRDD, tablename, curDate, "ftp");
        System.out.println("导入完成时间：>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + DateUtils.TIME_FORMAT.format(new Date()));
    }

    public static void insertHBase(JavaPairRDD<ImmutableBytesWritable, Put> hbasePairRDD, String tablename, Date date, String bcpType) {
        //获取Hbase的任务配置对象
        JobConf jobConf = HBaseUtil.getHbaseJobConf();
        //设置要插入的HBase表
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename);

        //将数据写入HBase
        short importStatus;
        try {
            hbasePairRDD.saveAsHadoopDataset(jobConf);
            importStatus = 1;
        } catch (Exception e) {
            e.printStackTrace();
            importStatus = -1;
        }
        ImportBcpRecord insertRecord = new ImportBcpRecord(null, bcpType, date, importStatus);

        IImportBcpRecordDao recordDao = DaoFactory.getImportBcpRecordDao();
        recordDao.insertRecord(insertRecord);
    }


}
