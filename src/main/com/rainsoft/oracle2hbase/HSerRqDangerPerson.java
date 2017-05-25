package com.rainsoft.oracle2hbase;

import com.rainsoft.util.java.Constants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2017-05-24.
 */
public class HSerRqDangerPerson {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("高危人群信息表数据从Oracle迁移到HBase")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        HiveContext sqlContext = new HiveContext(sc.sc());

        String url = Constants.ORACLE_URL;
        String username = Constants.ORACLE_USER;
        String passwd = Constants.ORACLE_PASSWORD;
        String driver = Constants.ORACLE_DRIVER;

        Map<String, String> options = new HashMap<>();
        options.put("url", url);
        options.put("dbtable", "ser_rq_danger_person");

        DataFrame personDF = sqlContext.read().format("jdbc").options(options).load();

        personDF.javaRDD().foreach(
                new VoidFunction<Row>() {
                    @Override
                    public void call(Row row) throws Exception {
                        System.out.println(row.toString());
                    }
                }
        );

        /*Properties prop = new Properties();
        prop.setProperty("user", username);
        prop.setProperty("password", passwd);

        Map<String, String> jdbcMap = JDBCUtils.getJDBCMap();
        jdbcMap.put("dbtable", "ser_rq_danger_person");
        DataFrame infoDF = sqlContext.read().jdbc(url, "", prop);*/


//        DataFrame dangerPersonDF = sqlContext.read().options(jdbcMap).load();


//        String sql = "select p.imsi as imsi, p.brief as pbrief, a.brief as abrief from dangerPerson p left join dangerArea a on p.area = a.area";
    }
}
