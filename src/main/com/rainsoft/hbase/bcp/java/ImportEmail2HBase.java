package com.rainsoft.hbase.bcp.java;

import com.rainsoft.util.java.DateUtils;
import com.rainsoft.util.java.FieldConstant;
import com.rainsoft.util.java.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

/**
 * Created by CaoWeidong on 2017-05-09.
 */
public class ImportEmail2HBase {
    public static void main(String[] args) throws ParseException, IOException {
        //HBase表名
        String tablename = args[0];
        //Hbase表列簇
        String cf = "CONTENT_EMAIL";
        //bcp表字段
        String[] columns = FieldConstant.HBASE_FIELD_MAP.get("email");

        String bcpPath = args[1];

        //导入HBase
        importEmailJob(tablename, cf, columns, bcpPath);

        //删除源数据
        FileUtils.delChildFile(new File(bcpPath));
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
    public static void importEmailJob(String tablename, String cf, String[] columns, String bcpPath) throws ParseException {

        SparkConf conf = new SparkConf()
                .setAppName("import email pcb data into HBase");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> originalRDD = sc.textFile("file://" + bcpPath);

        System.out.println("开始导入时间：>>>>>>>>>>>>>>>>>>>>>>>> " + DateUtils.TIME_FORMAT.format(new Date()) + " <<<<<<<<<<<<<<<<<<<<<<<<<<<<<");

        //按字段切分
        JavaRDD<Row> emailRowRDD = originalRDD.map(
                (Function<String, Row>) str -> RowFactory.create(str.replace("\\|$\\|", "").split("\\|#\\|"))
        );

        //过滤
        JavaRDD<Row> filterEmailRDD = emailRowRDD.filter(
                (Function<Row, Boolean>) v1 -> {
                    if (v1.length() < columns.length)
                        return false;
                    else {
                        String capTime = v1.getString(24);
                        if (DateUtils.isDate(capTime, DateUtils.TIME_FORMAT) == false) {
                            return false;
                        }
                    }
                    return true;
                }
        );
        /**
         *导入HBase
         */
        ImportBcp2HBase.import2HBase(tablename, cf, columns, filterEmailRDD);
    }
}
