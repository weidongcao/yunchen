package com.rainsoft.util.java;

import org.apache.hadoop.conf.Configuration;

import java.util.Properties;

/**
 * 常量类
 * Created by Administrator on 2017-04-10.
 */
public class PropConstants {

    /*
     *集群相关常量
     */
    //HBase的Zookeeper集群节点
    public static String HBASE_ZOOKEEPER_QUORUM = "hbase_zookeeper_quorum";
    //Zookeeper集群端口
    public static String HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT = "hbase_zookeeper_property_client_port";

    //HBase的Master节点
    public static String HBASE_MASTER = "hbase_master";

    //HBase的Region Server共享目录
    public static String HBASE_ROOTDIR = "hbase_rootdir";

    // 需要建立Solr索引的HBase表名称
    public static String HBASE_TABLE_NAME = "hbase_table_name";

    // HBase表的列簇
    public static String HBASE_TABLE_FAMILY = "hbase_table_family";

    //查询字段
    public static String QUERY_FIELD = "query_field";

    //Solr的核心（集合）
    public static String COLLECTION = "collection";

    /*
     * 数据库连接
     */
    //JDBC驱动
    public static String JDBC_DRIVER = "jdbc_driver";
    //JDBC连接池大小
    public static String JDBC_DATASOURCE_SIZE = "jdbc_datasource_size";
    //JDBC连接URL
    public static String JDBC_URL = "jdbc_url";
    //JDBC连接用户名
    public static String JDBC_USER = "jdbc_username";
    //JDBC连接密码
    public static String JDBC_PASSWORD = "jdbc_password";

    /*
     * 业务相关常量
     */
    //昨天出现权重
    public static String WEIGHT_YESTERDAY = "weight_yesterday";

    //再出现要增加的权重
    public static String WEIGHT_ADD = "weight_add";

    //不出现要减的权重
    public static String WEIGHT_REDUCE = "weight_reduce";

    //BCP文件所在目录
    public static String INPUT_DATA_PATH = "input_data_path";

    //小区实有人群
    public static String TOTAL_PEOPLE = "total_people";

    //小区常住人群
    public static String REGULAR_PEOPLE = "regular_people";

    //小区暂停人群
    public static String TEMPORARY_PEOPLE = "temporary_people";

    //小区闪现人群
    public static String SELDOM_PEOPLE = "seldom_people";

    //统计的天数
    public static String COUNT_DAYS = "count_days";

    //人群分析表
    public static String HTABLE_PEOPLE_ANALYSIS = "htable_people_analysis";

    //人群分析表列簇
    public static String CF_PEOPLE_ANALYSIS = "cf_people_analysis";

    //小区分析表
    public static String HTABLE_COMMUNITY_ANALYSIS = "htable_community_analysis";

    //小区分析表列簇
    public static String CF_COMMUNITY_ANALYSIS = "cf_community_analysis";

    //重点区域刷新时长
    public static String EMPHASIS_TIME_INTERVAL = "emphasis_time_interval";

    //重点区域人群分析表
    public static String HTABLE_EMPHASIS_ANALYSIS = "htable_emphasis_analysis";

    //重点区域默认周期
    public static String EMPHASIS_DOUBTFUL_PERIOD = "emphasis_doubtful_period";

    //重点区域最大周期
    public static String EMPHASIS_MAX_PERIOD = "emphasis_max_period";

    //重点区域默认天数
    public static String EMPHASIS_DOUBTFUL_DAYS = "emphasis_doubtful_days";

    //重点区域默认次数
    public static String EMPHASIS_DOUBTFUL_TIME2 = "emphasis_doubtful_time2";

    //将BCP数据文件导入到HBase的时间间隔
    public static String IMPORT_BCP_DATA_TIME_INTERVAL = "import_bcp_data_time_interval";

    //BCP文件所有目录
    public static String DIR_BCP_RESOURCE = "dir_bcp_resource";

    //FTP内容表的字段
    public static String FIELD_REG_CONTENT_FTP = "field_reg_content_ftp";
}
