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

import java.io.File;
import java.text.ParseException;
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
public class ImportBcp2HBase {
    public static void main(String[] args) throws ParseException {
        String tablename = args[0];
        String cf = args[1];
        int colCount = Integer.valueOf(args[2]);
        String bcpPath = args[3];
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>导入的表: " + tablename + " <<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>列簇:\t " + cf + " <<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>列数:\t " + colCount + " <<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>bcp路径: " + bcpPath + " <<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
//        importBcpJob("H_REG_CONTENT_HTTP_TMP", "CONTENT_HTTP", 27, "file:///D:\\0WorkSpace\\Develop\\data\\bcp-http");

        importBcpJob(tablename, cf, colCount, bcpPath);

        //删除源数据
        File inpath = new File(bcpPath);
        String[] fileList = inpath.list();
        for (String file : fileList) {
            FileUtils.deleteDir(new File(inpath, file));
        }
    }

    public static void importBcpJob(String tablename, String cf, int colCount, String bcpPath) throws ParseException  {

        SparkConf conf = new SparkConf()
                .setAppName("import pcb data into HBase")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JSONArray ftpTableFields = JSONArray.fromObject(Constants.FIELD_H_REG_CONTENT_FTP);

        JavaRDD<String> originalRDD = sc.textFile("file://" + bcpPath);

        System.out.println("开始导入时间：>>>>>>>>>>>>>>>>>>>>>>>> " + DateUtils.TIME_FORMAT.format(new Date()) + " <<<<<<<<<<<<<<<<<<<<<<<<<<<<<");

        //按字段切分
        JavaRDD<Row> ftpRowRDD = originalRDD.map(
                (Function<String, Row>) str -> RowFactory.create(str.replace("\\|$\\|", "").split("\\|#\\|"))
        );

        //过滤
        JavaRDD<Row> filterFtpRDD = ftpRowRDD.filter(
                (Function<Row, Boolean>) v1 -> {
                    if (v1.length() == colCount) return true;
                    else return false;
                }
        );

        //转换为HBase的数据格式
        JavaPairRDD<ImmutableBytesWritable, Put> hbasePairRDD = filterFtpRDD.mapToPair(
                (PairFunction<Row, ImmutableBytesWritable, Put>) row -> new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), HBaseUtil.createHBasePut(row, ftpTableFields, cf))
        );

        //导入HBase
        insertHBase(hbasePairRDD, tablename);

        System.out.println("导入完成时间：>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> " + DateUtils.TIME_FORMAT.format(new Date()) + " <<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
    }

    public static void insertHBase(JavaPairRDD<ImmutableBytesWritable, Put> hbasePairRDD, String tablename) {
        //获取Hbase的任务配置对象
        JobConf jobConf = HBaseUtil.getHbaseJobConf();
        //设置要插入的HBase表
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename);

        //将数据写入HBase
        hbasePairRDD.saveAsHadoopDataset(jobConf);
    }
}
