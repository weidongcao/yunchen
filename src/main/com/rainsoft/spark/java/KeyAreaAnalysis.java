package com.rainsoft.spark.java;

import com.rainsoft.util.java.IMSIUtils;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by Administrator on 2017-04-24.
 */
public class KeyAreaAnalysis {
    public static void main(String[] args) throws IOException {
        analysisAreaPeople();
    }

    public static void analysisAreaPeople() throws IOException {
        SparkConf conf = new SparkConf()
                .setAppName("重点小区人群分析")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

//        HiveContext hiveContext = new HiveContext(sc.sc());

        JavaRDD<String> improveDataRDD = sc.textFile("file:///D:\\0WorkSpace\\Develop\\data\\scan_ending_improve.bcp");

        String type = "local_window";
        if (type.startsWith("local")) {
            JavaRDD<String> phoneAreaRDD = null;
            //读取手机号前7位相关信息
            if (type.startsWith("local_window")) {
                phoneAreaRDD = sc.textFile("file:///D:\\0WorkSpace\\Develop\\data\\phone_to_area_30w");
            } else if (type.equals("local_linux")) {
                phoneAreaRDD = sc.textFile("file:///phone_to_area_30w");
            }

            /*
             * 将手机号前7位及其相关信息注册为临时表
             */
            //手机号前7位表数据
            JavaRDD<Row> phoneAreaRowRDD = phoneAreaRDD.map(
                    (Function<String, Row>) s -> {
                        String[] arr = s.split("\t");
                        return RowFactory.create(arr[0], arr[1], arr[2], arr[3], arr[4], arr[5]);
                    }
            );

            //手机号前7位表元数据
            StructType phoneSchema = DataTypes.createStructType(Arrays.asList(
                    //ID
                    DataTypes.createStructField("id", DataTypes.StringType, true),
                    //手机号前7位字段
                    DataTypes.createStructField("phone_num", DataTypes.StringType, true),
                    //地区名字段
                    DataTypes.createStructField("area_name", DataTypes.StringType, true),
                    //地区代码字段
                    DataTypes.createStructField("area_code", DataTypes.StringType, true),
                    //运营商字段
                    DataTypes.createStructField("phone_type", DataTypes.StringType, true),
                    //电话区号字段
                    DataTypes.createStructField("region", DataTypes.StringType, true)
            ));

            //创建DataFrame对象
            DataFrame phoneDF = sqlContext.createDataFrame(phoneAreaRowRDD, phoneSchema);

            //注册为临时表
            phoneDF.registerTempTable("phone2area");

        }

        /*
         * 通过IMSI号生成手机号前7位，并注册为临时表
         */
        //生成IMSI临时表数据
        JavaRDD<Row> improveRowRDD = improveDataRDD.map(
                new Function<String, Row>() {
                    @Override
                    public Row call(String s) throws Exception {
                        s = s.replace("\\|$\\|", "");
                        String[] arr = s.split("\\|#\\|");
                        String phone7 = IMSIUtils.getMobileAll(arr[1]);
                        if ((arr != null) && (arr.length == 14)) {
                            return RowFactory.create(
                                    arr[0],
                                    arr[1],
                                    phone7,
                                    arr[2],
                                    arr[3],
                                    arr[4]
                            );
                        } else {
                            return null;
                        }
                    }
                }
        );

        //生成IMSI临时表元数据
        StructType improveSchema = DataTypes.createStructType(Arrays.asList(
                //手机号前7位的表头
                DataTypes.createStructField("equipment_mac", DataTypes.StringType, true),
                DataTypes.createStructField("imsi_code", DataTypes.StringType, true),
                DataTypes.createStructField("phone_num", DataTypes.StringType, true),
                DataTypes.createStructField("capture_time", DataTypes.StringType, true),
                DataTypes.createStructField("operator_type", DataTypes.StringType, true),
                DataTypes.createStructField("sn_code", DataTypes.StringType, true)
        ));

        //创建DataFrame
        DataFrame imsiDF = sqlContext.createDataFrame(improveRowRDD, improveSchema);

        //注册为临时表
        imsiDF.registerTempTable("improve");


        //两个表join的sql
        String sql = "select i.imsi_code, p.phone_num, p.area_name, p.area_code, p.phone_type, p.region, i.capture_time, i.sn_code from improve i left join phone2area p on i.phone_num = p.phone_num";

        //两个临时表进行join
        DataFrame infoDF = sqlContext.sql(sql);

        JavaRDD<Row> infoRDD = infoDF.javaRDD();

        File keyarea = new File("keyarea.txt");
        if (keyarea.exists()) {
            keyarea.delete();
        }
        keyarea.createNewFile();

        infoRDD.foreach(
                new VoidFunction<Row>() {
                    @Override
                    public void call(Row row) throws Exception {
                        String line = row.getString(0) + "\t" + row.getString(1) + "\t" + row.getString(2) + "\t" + row.getString(3) + "\t" + row.getString(4) + "\t" + row.getString(5) + "\t" + row.getString(6) + "\t" + row.getString(7);
                        FileUtils.writeStringToFile(keyarea, line + "\n", true);
                    }
                }
        );
//        infoDF.registerTempTable("improve_phone");

    }
}
