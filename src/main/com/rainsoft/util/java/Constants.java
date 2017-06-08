package com.rainsoft.util.java;

import net.sf.json.JSONArray;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2017-05-09.
 */
public class Constants {
    /**
     * 业务相关常量
     */

    //统计的天数
    public static final int COUNT_DAYS = 90;

    //昨天出现权重
    public static final Float WEIGHT_YESTERDAY = 100f;

    //增加的权重
    public static final Float WEIGHT_ADD = 1f;

    //减少的权重
    public static final Float WEIGHT_REDUCE = 0.01f;

    /**
     * 小区人群表
     */
    //小区实有人群
    public static final String TOTAL_PEOPLE = "total_people";

    //小区常住人群
    public static final String REGULAR_PEOPLE = "regular_people";

    //小区暂停人群
    public static final String TEMPORARY_PEOPLE = "temporary_people";

    //小区闪现人群
    public static final String SELDOM_PEOPLE = "seldom_people";

    /**
     * Mysql数据库连接
     */
    //JDBC驱动
    public static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    //JDBC连接池大小
    public static final int MYSQL_DATASOURCE_SIZE = 6;
    //JDBC连接URL
    public static final String MYSQL_URL = "jdbc:mysql://nn1.hadoop.com:3306/rsdb";
    //JDBC连接用户名
    public static final String MYSQL_USER = "root";
    //JDBC连接密码
    public static final String MYSQL_PASSWORD = "rainsoft";

    /**
     * Oracle数据库连接
     */
    //JDBC驱动
    public static final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
    //JDBC连接池大小
    public static final int ORACLE_DATASOURCE_SIZE = 6;
    //JDBC连接URL
    public static final String ORACLE_URL = "jdbc:oracle:thin:@192.168.30.205:1521:rsdb";
    //JDBC连接用户名
    public static final String ORACLE_USER = "gz_inner";
    //JDBC连接密码
    public static final String ORACLE_PASSWORD = "gz_inner";

    /**
     * Solr相关常量
     */
    public static final String MYSQL_COLLECTION = "hbase";

    /**
     * 重点区域相关常量
     */
    //重点区域默认周期
    public static final int EMPHASIS_DOUBTFUL_PERIOD = 5;

    //重点区域最大周期
    public static final int EMPHASIS_MAX_PERIOD = 10;

    //重点区域默认天数
    public static final int EMPHASIS_DOUBTFUL_DAYS = 3;

    //重点区域默认次数
    public static final int EMPHASIS_DOUBTFUL_TIMES = 7;

    //Spark 生成HFile文件的临时存储目录
    public static final String HFILE_TEMP_STORE_PATH = "hdfs://dn1.hadoop.com:8020/user/root/hbase/table/";

    //Spark 生成HDFS文件的临时存储目录
    public static final String HIVE_TABLE_HDFS_DATA_TEMP_STORE_PATH = "hdfs://dn1.hadoop.com:8020/user/root/hive/table/";

}
