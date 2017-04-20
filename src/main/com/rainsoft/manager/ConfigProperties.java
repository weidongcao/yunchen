package com.rainsoft.manager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * Created by Administrator on 2017-04-06.
 */
public class ConfigProperties {
    public static Logger logger = LoggerFactory.getLogger(ConfigProperties.class);
    private static Properties props;

    //HBase的Zookeeper集群节点
    private static String HBASE_ZOOKEEPER_QUORUM;

    //Zookeeper集群端口
    private static String HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT;

    //HBase的Master节点
    private static String HBASE_MASTER;

    //HBase的Region Server共享目录
    private static String HBASE_ROOTDIR;

    // 需要建立Solr索引的HBase表名称
    private static String HBASE_TABLE_FAMILY;

    // HBase表的列簇
    private static String HBASE_TABLE_NAME;

    //查询字段
    private static String QUERY_FIELD;

    //Solr的核心（集合）
    private static String COLLECTION;

    //Hadoop配置项
    private static Configuration conf;

    /**
     * 从配置文件读取并设置HBase配置信息
     *
     * @param propsLocation
     * @return
     */
    static {
        props = new Properties();
        try {
            InputStream in = ConfigProperties.class.getClassLoader().getResourceAsStream("config.properties");
            props.load(new InputStreamReader(in, "UTF-8"));

            HBASE_ZOOKEEPER_QUORUM = props.getProperty("HBASE_ZOOKEEPER_QUORUM");
            HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT = props.getProperty("HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT");
            HBASE_MASTER = props.getProperty("HBASE_MASTER");
            HBASE_ROOTDIR = props.getProperty("HBASE_ROOTDIR");
            HBASE_TABLE_NAME = props.getProperty("HBASE_TABLE_NAME");
            HBASE_TABLE_FAMILY = props.getProperty("HBASE_TABLE_FAMILY");
            QUERY_FIELD = props.getProperty("QUERY_FIELD");

            COLLECTION = props.getProperty("COLLECTION");
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM);
            conf.set("hbase.zookeeper.property.clientPort", HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT);
            conf.set("hbase.master", HBASE_MASTER);
            conf.set("hbase.rootdir", HBASE_ROOTDIR);
            conf.set("mapreduce.job.user.classpath.first", "true");
            conf.set("mapreduce.task.classpath.user.precedence", "true");


        } catch (IOException e) {
            logger.error("加载配置文件出错", e);
        } catch (NullPointerException e) {
            logger.error("加载文件出错", e);
        } catch (Exception e) {
            logger.error("加载配置文件出现位置异常", e);
        }
    }

    public static Logger getLogger() {
        return logger;
    }

    public static Properties getProps() {
        return props;
    }

    public static String getHBASE_ZOOKEEPER_QUORUM() {
        return HBASE_ZOOKEEPER_QUORUM;
    }

    public static String getHBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT() {
        return HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT;
    }

    public static String getHBASE_MASTER() {
        return HBASE_MASTER;
    }

    public static String getHBASE_ROOTDIR() {
        return HBASE_ROOTDIR;
    }

    public static String getHBASE_TABLE_NAME() {
        return HBASE_TABLE_NAME;
    }

    public static String getHBASE_TABLE_FAMILY() {
        return HBASE_TABLE_FAMILY;
    }

    public static String getQUERY_FIELD() {
        return QUERY_FIELD;
    }

    public static Configuration getConf() {
        return conf;
    }

    public static String getCOLLECTION() {
        return COLLECTION;
    }

    public static void setCOLLECTION(String cOLLECTION) {
        COLLECTION = cOLLECTION;
    }



}