package com.rainsoft.hbase.java;

import com.rainsoft.util.java.DateUtils;
import com.rainsoft.util.java.FieldConstant;
import com.rainsoft.util.java.FileUtils;
import com.rainsoft.util.java.HBaseUtil;
import com.rainsoft.util.scala.BcpUtil;
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
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

/**
 * Created by CaoWeidong on 2017-05-09.
 */
public class ImportBcp2HBase {
    public static void main(String[] args) throws ParseException, IOException {
        //HBase表名
        String tablename = args[0];
        //Hbase表列簇
        String cf = args[1];
        //bcp文件类型
        String bcpType = args[2];
        String[] columns = FieldConstant.HBASE_FIELD_MAP.get(bcpType);

        String bcpPath = args[3];

        if ("im_chat".equals(bcpType)) {
            bcpPath = FileUtils.convertFilContext(bcpPath);

        }
        System.out.println("导入的表: " + tablename + "\t|列簇:	 " + cf + "\t|数据类型: " + bcpType + "\t|bcp路径: " + bcpPath);
        //导入HBase
        importBcpJob(tablename, cf, columns, bcpPath);

        //删除源数据
        File inpath = new File(bcpPath);
        if (null != inpath) {
            String[] fileList = inpath.list();
            for (String file : fileList) {
                FileUtils.deleteDir(new File(inpath, file));
            }
        }
        System.out.println("导入完成时间：>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> " + DateUtils.TIME_FORMAT.format(new Date()) + " <<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
    }

    /**
     * 将BCP文件导入到HBase
     *
     * @param tablename
     * @param cf
     * @param columns
     * @param bcpPath
     * @throws ParseException
     */
    public static void importBcpJob(String tablename, String cf, String[] columns, String bcpPath) throws ParseException {

        SparkConf conf = new SparkConf()
                .setAppName("import pcb data into HBase")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> originalRDD = sc.textFile("file://" + bcpPath);

        System.out.println("开始导入时间：>>>>>>>>>>>>>>>>>>>>>>>> " + DateUtils.TIME_FORMAT.format(new Date()) + " <<<<<<<<<<<<<<<<<<<<<<<<<<<<<");

        //按字段切分
        JavaRDD<Row> ftpRowRDD = originalRDD.map(
                (Function<String, Row>) str -> RowFactory.create(str.replace("\\|$\\|", "").split("\\|#\\|"))
        );

        //过滤
        JavaRDD<Row> filterFtpRDD = ftpRowRDD.filter(
                (Function<Row, Boolean>) v1 -> {
                    if (v1.length() == columns.length) return true;
                    else return false;
                }
        );

        //转换为HBase的数据格式
        JavaPairRDD<ImmutableBytesWritable, Put> hbasePairRDD = filterFtpRDD.mapToPair(
                (PairFunction<Row, ImmutableBytesWritable, Put>) row -> new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), HBaseUtil.createHBasePut(row, columns, cf))
        );

        /**
         *导入HBase
         */
        //获取Hbase的任务配置对象
        JobConf jobConf = HBaseUtil.getHbaseJobConf();
        //设置要插入的HBase表
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename);

        //将数据写入HBase
        hbasePairRDD.saveAsHadoopDataset(jobConf);
    }
}
