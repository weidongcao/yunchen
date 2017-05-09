package com.rainsoft.util.java;

import net.sf.json.JSONArray;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

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
    public static void init() throws IOException {
        // 获取HBase配置信息
        conf = HBaseConfiguration.create();

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

    public static Put createHBasePut(Row row, JSONArray fieldArray, String cf) {
        String uuid = UUID.randomUUID().toString().replace("-", "");
        Put put = new Put(Bytes.toBytes(uuid));
        for (int i = 0; i < row.length(); i++) {
            if ((null != row.getString(i)) && ("".equals(row.getString(i)) == false)) {
                HBaseUtil.addHBasePutColumn(put, cf, fieldArray.optString(i), row.getString(i));
            }
        }
        return put;
    }

    public static Put addHBasePutColumn(Put put, String cf, String col, String value) {
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(col), Bytes.toBytes(value));
        return put;
    }
}
