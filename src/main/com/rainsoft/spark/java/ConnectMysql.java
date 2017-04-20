package com.rainsoft.spark.java;

import com.rainsoft.manager.ConfManager;
import com.rainsoft.util.java.Constants;
import com.rainsoft.util.java.StringUtils;
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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created by Administrator on 2017-04-20.
 */
public class ConnectMysql {
    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf()
                .setAppName("测试Spark读取Mysql的数据")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        String url = ConfManager.getProperty(Constants.JDBC_URL);
        String username = ConfManager.getProperty(Constants.JDBC_USER);
        String passwd = ConfManager.getProperty(Constants.JDBC_PASSWORD);
        String driver = ConfManager.getProperty(Constants.JDBC_DRIVER);

        Properties prop = new Properties();
        prop.setProperty("user", username);
        prop.setProperty("password", passwd);

        JavaRDD<String> originalRDD = sc.textFile("file:///D:\\0WorkSpace\\Develop\\data\\comprehensive_20170310");

        //测试代码
        JavaRDD<Row> comprehensiveDataRDD = originalRDD.map(
                new Function<String, Row>() {
                    @Override
                    public Row call(String s) throws Exception {
                        String[] str = s.split("\t");
                        return RowFactory.create(
                                StringUtils.replaceNull(str[0]),
                                StringUtils.replaceNull(str[1]),
                                StringUtils.replaceNull(str[2]),
                                StringUtils.replaceNull(str[3]),
                                StringUtils.replaceNull(str[4]),
                                StringUtils.replaceNull(str[5]),
                                StringUtils.replaceNull(str[6]),
                                StringUtils.replaceNull(str[7]),
                                "null".equals(str[8]) ? null : Float.valueOf(str[8]),
                                StringUtils.replaceNull(str[9]),
                                StringUtils.replaceNull(str[10]),
                                StringUtils.replaceNull(str[11]),
                                StringUtils.replaceNull(str[12]),
                                StringUtils.replaceNull(str[13])
                        );
                    }
                }
        );

        StructType comprehensiveSchema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("yest_imsi", DataTypes.StringType, true),
                DataTypes.createStructField("hist_imsi", DataTypes.StringType, true),
                DataTypes.createStructField("yest_service_name", DataTypes.StringType, true),
                DataTypes.createStructField("hist_service_name", DataTypes.StringType, true),
                DataTypes.createStructField("yest_service_code", DataTypes.StringType, true),
                DataTypes.createStructField("hist_service_code", DataTypes.StringType, true),
                DataTypes.createStructField("yest_machine_id", DataTypes.StringType, true),
                DataTypes.createStructField("hist_amchine_id", DataTypes.StringType, true),
                DataTypes.createStructField("hist_weight", DataTypes.FloatType, true),
                DataTypes.createStructField("yest_phone7", DataTypes.StringType, true),
                DataTypes.createStructField("yest_area_name", DataTypes.StringType, true),
                DataTypes.createStructField("yest_area_code", DataTypes.StringType, true),
                DataTypes.createStructField("yest_phone_type", DataTypes.StringType, true),
                DataTypes.createStructField("yest_region", DataTypes.StringType, true)
        ));

        DataFrame comprehensiveDF = sqlContext.createDataFrame(comprehensiveDataRDD, comprehensiveSchema);

        comprehensiveDF.registerTempTable("comprehensive");

        DataFrame dangerPersonDF = sqlContext.read().jdbc(url, "ser_rq_danger_person", prop);

        dangerPersonDF.registerTempTable("dangerPerson");

        DataFrame dangerAreaDF = sqlContext.read().jdbc(url, "ser_rq_danger_area", prop);

        dangerAreaDF.registerTempTable("dangerArea");

//        String sql = "select p.imsi as imsi, p.brief as pbrief, a.brief as abrief from dangerPerson p left join dangerArea a on p.area = a.area";
        File dangerFile = new File("sql/danger.sql");
        String sql = FileUtils.readFileToString(dangerFile);
        DataFrame dangerDF = sqlContext.sql(sql);
        JavaRDD<Row> dangerRDD = dangerDF.javaRDD();
        File communityDangerFile = new File("community_danger.txt");
        if (communityDangerFile.exists()) {
            communityDangerFile.delete();
        }
        communityDangerFile.createNewFile();
        dangerRDD.foreach(
                new VoidFunction<Row>() {
                    @Override
                    public void call(Row row) throws Exception {
                        String line = row.getString(0) + "\t" + row.getString(1) + "\t" + row.getString(2) + "\t" + row.getString(3);
                        FileUtils.writeStringToFile(communityDangerFile, line + "\r\n", true);
                    }
                }
        );
    }
}
