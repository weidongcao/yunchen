package com.rainsoft.hbase.bcp.java;

import com.rainsoft.util.java.DateUtils;
import com.rainsoft.util.java.FieldConstant;
import com.rainsoft.util.java.FileUtils;
import com.rainsoft.util.java.NumberUtils;
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
 *
 * Created by CaoWeidong on 2017-05-09.
 */
public class ImportService2HBase {
    public static void main(String[] args) throws ParseException, IOException {
        //HBase表名
        String tablename = args[0];
        //Hbase表列簇
        String cf = "SERVICE_INFO";
        //bcp表字段
        String[] columns = FieldConstant.HBASE_FIELD_MAP.get("service");

        String bcpPath = args[1];
        //导入HBase
        importBcpJob(tablename, cf, columns, bcpPath);

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
    public static void importBcpJob(String tablename, String cf, String[] columns, String bcpPath) throws ParseException {

        SparkConf conf = new SparkConf()
                .setAppName("import service pcb data into HBase");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> originalRDD = sc.textFile("file://" + bcpPath);

        System.out.println("开始导入时间：>>>>>>>>>>>>>>>>>>>>>>>> " + DateUtils.TIME_FORMAT.format(new Date()) + " <<<<<<<<<<<<<<<<<<<<<<<<<<<<<");

        //按字段切分
        JavaRDD<Row> serviceRowRDD = originalRDD.map(
                (Function<String, Row>) str -> RowFactory.create(str.replace("\\|$\\|", "").split("\\|#\\|"))
        );

        //过滤
        JavaRDD<Row> filterServiceRDD = serviceRowRDD.filter(
                (Function<Row, Boolean>) row -> {
                    if (row.length() < columns.length) {
                        return false;
                    } else {
                        String postalCode = row.getString(3);
                        String serverCount = row.getString(12);
                        String real_ending_nums = row.getString(44);
                        if (NumberUtils.isDigit(postalCode) == false) {
                            return false;
                        } else if (NumberUtils.isDigit(serverCount) == false) {
                            return false;
                        } else if (NumberUtils.isDigit(real_ending_nums) == false) {
                            return false;
                        }
                    }
                    return true;
                }
        );

        /**
         *导入HBase
         */
        ImportBcp2HBase.import2HBase(tablename, cf, columns, filterServiceRDD);
    }
}
