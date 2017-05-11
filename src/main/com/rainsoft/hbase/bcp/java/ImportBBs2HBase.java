package com.rainsoft.hbase.bcp.java;

import com.rainsoft.util.java.*;
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

import static com.rainsoft.hbase.bcp.java.ImportBcp2HBase.import2HBase;

/**
 * Created by CaoWeidong on 2017-05-09.
 */
public class ImportBBs2HBase {
    public static void main(String[] args) throws ParseException, IOException {
        //HBase表名
        String tablename = args[0];
        //Hbase表列簇
        String cf = "CONTENT_BBS";
        //bcp表字段
        String[] columns = FieldConstant.HBASE_FIELD_MAP.get("bbs");

        String bcpPath = args[1];
        //导入HBase
        importBbsJob(tablename, cf, columns, bcpPath);

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
    public static void importBbsJob(String tablename, String cf, String[] columns, String bcpPath) throws ParseException {

        SparkConf conf = new SparkConf()
                .setAppName("import weibo pcb data into HBase");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> originalRDD = sc.textFile("file://" + bcpPath);
        originalRDD.collect();
        System.out.println("开始导入时间：>>>>>>>>>>>>>>>>>>>>>>>> " + DateUtils.TIME_FORMAT.format(new Date()) + " <<<<<<<<<<<<<<<<<<<<<<<<<<<<<");

        //按字段切分
        JavaRDD<Row> ftpRowRDD = originalRDD.map(
                (Function<String, Row>) str -> RowFactory.create(str.replace("\\|$\\|", "").split("\\|#\\|"))
        );

        //过滤
        JavaRDD<Row> filterBbsRDD = ftpRowRDD.filter(
                new Function<Row, Boolean>() {
                    @Override
                    public Boolean call(Row row) throws Exception {
                        if (row.length() < columns.length) {
                            return false;
                        } else {
                            String serviceType = row.getString(5);
                            String actionType = row.getString(16);
                            String capTime = row.getString(24);

                            if (NumberUtils.isDigit(serviceType) == false) {
                                return false;
                            } else if (NumberUtils.isDigit(actionType) == false) {
                                return false;
                            } else if (DateUtils.isDate(capTime, DateUtils.TIME_FORMAT) == false) {
                                return false;
                            }
                        }
                        return true;
                    }
                }
        );
        import2HBase(tablename, cf, columns, filterBbsRDD);
    }
}
