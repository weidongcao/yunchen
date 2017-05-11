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
 * Created by CaoWeidong on 2017-05-09.
 */
public class ImportWeibo2HBase {
    public static void main(String[] args) throws ParseException, IOException {
        //HBase表名
        String tablename = args[0];
        //Hbase表列簇
        String cf = "CONTENT_WEIBO";
        //bcp表字段
        String[] columns = FieldConstant.HBASE_FIELD_MAP.get("weibo");

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
                .setAppName("import weibo pcb data into HBase");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> originalRDD = sc.textFile("file://" + bcpPath);

        System.out.println("开始导入时间：>>>>>>>>>>>>>>>>>>>>>>>> " + DateUtils.TIME_FORMAT.format(new Date()) + " <<<<<<<<<<<<<<<<<<<<<<<<<<<<<");

        //按字段切分
        JavaRDD<Row> weiboRowRDD = originalRDD.map(
                (Function<String, Row>) str -> RowFactory.create(str.replace("\\|$\\|", "").split("\\|#\\|"))
        );

        //过滤
        JavaRDD<Row> filterWeiboRDD = weiboRowRDD.filter(
                (Function<Row, Boolean>) row -> {
                    if (row.length() < columns.length)
                        return false;
                    else {
                        String capTime = row.getString(24);
                        String actionType = row.getString(16);
                        if (DateUtils.isDate(capTime, DateUtils.TIME_FORMAT) == false) {
                            return false;
                        } else if (NumberUtils.isDigit(actionType) == false) {
                            return false;
                        }
                    }
                    return true;
                }
        );

        /**
         *导入HBase
         */
        ImportBcp2HBase.import2HBase(tablename, cf, columns, filterWeiboRDD);
    }
}
