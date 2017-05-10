package com.rainsoft.util.java;

/**
 * Created by Administrator on 2017-05-09.
 */
public class Constants {
    //FTP内容表的字段
    public static final String FIELD_H_REG_CONTENT_FTP = "['SERVICEID', 'SESSIONID', 'CERTIFICATE_TYPE', 'CERTIFICATE_CODE', 'USERNAME', 'SERVICETYPE', 'UNAME', 'PASSWORD', 'FILENAME', 'FILE', 'OPTYPE', 'REMOTEIP', 'IS_COMPLETE', 'REMOTEPORT', 'COMPUTERIP', 'COMPUTERPORT', 'COMPUTERMAC', 'CAPTIME', 'ROOM_ID', 'CHECKIN_ID', 'MACHINE_ID', 'DATA_SOURCE', 'FILESIZE', 'FILEURL']";

    //HTTP内容表的字段
    public static final String FIELD_H_REG_CONTENT_HTTP = "['SERVICEID', 'SESSIONID', 'CERTIFICATE_TYPE', 'CERTIFICATE_CODE', 'USERNAME', 'SERVICETYPE', 'URL', 'HOST', 'REFURL', 'REFHOST', 'ACTIONTYPE', 'SUBJECT', 'SUMMARY', 'COOKIE', 'ZIPPATH', 'UPFILE', 'DOWNFILE', 'REMOTEIP', 'REMOTEPORT', 'COMPUTERIP', 'COMPUTERPORT', 'COMPUTERMAC', 'CAPTIME', 'ROOM_ID', 'CHECKIN_ID', 'MACHINE_ID', 'DATA_SOURCE']";

    /**
     * 业务相关常量
     */

    //统计的天数
    public static final int COUNT_DAYS = 90;

    //昨天出现权重
    public static final Float WEIGHT_YESTERDAY = 100f;

    //再出现要增加的权重
    public static final Float WEIGHT_ADD = 1f;

    //不出现要减的权重
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
     * 数据库连接
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
}
