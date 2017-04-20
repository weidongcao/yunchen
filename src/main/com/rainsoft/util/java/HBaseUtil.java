package com.rainsoft.util.java;

import com.rainsoft.manager.ConfManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

/**
 * HBase工具类
 *
 * @author Cao Wei Dong
 * @time 2017-04-06
 */
public class HBaseUtil {

    private static Configuration conf = null;

    private static Connection conn = null;

    static {
        try {
            init();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 初始化HBase连接
     *
     * @throws IOException
     */
    public static void init1() throws IOException {
        // 获取HBase配置信息
        conf = HBaseConfiguration.create();

        //指定HBase的Zookeeper集群
//        conf.set(Constants.HBASE_ZOOKEEPER_QUORUM, ConfManager.getProperty(Constants.HBASE_ZOOKEEPER_QUORUM));
        //指定Zookeeper集群端口
        conf.set(Constants.HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT, ConfManager.getProperty(Constants.HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT));

        //指定HBase集群Master节点
        conf.set(Constants.HBASE_MASTER, ConfManager.getProperty(Constants.HBASE_MASTER));

        //创建HBase连接
        conn = ConnectionFactory.createConnection(conf);
    }
    public static void init() throws IOException {
        // 获取HBase配置信息
        conf = HBaseConfiguration.create();

        conf.addResource("hbase-site.xml");

        //创建HBase连接
        conn = ConnectionFactory.createConnection(conf);
    }
    /**
     * 获取HBase表名
     * @param tableName HBase表名
     * @return HBase表
     * @throws Exception
     */
    public static Table getTable(String tableName) throws Exception {

        Table table = conn.getTable(TableName.valueOf(tableName));

        return table;
    }

    public static JobConf getHbaseJobConf() {
        JobConf jobConf = new JobConf(conf);
        jobConf.setOutputFormat(TableOutputFormat.class);

        return jobConf;
    }
}
