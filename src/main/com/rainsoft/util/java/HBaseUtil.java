package com.rainsoft.util.java;

import com.rainsoft.hbase.hfile.java.RowkeyColumnSecondarySort;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * HBase工具类
 *
 * @author Cao Wei Dong
 * @time 2017-04-06
 */
public class HBaseUtil {

    //HBase 配置类
    private static Configuration conf = null;

    //HBase连接
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
        conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily",10000);
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        conf.set("hbase.zookeeper.quorum", "nn1,nn2,dn1,dn2,dn3");
//        conf.set("hbase.zookeeper.master","dn3:60000");

        //创建HBase连接
        conn = ConnectionFactory.createConnection(conf);
    }

    /**
     * 获取HBase表名
     *
     * @param tableName HBase表名
     * @return HBase表
     * @throws Exception
     */
    public static HTable getTable(String tableName) throws Exception {

        Table table = conn.getTable(TableName.valueOf(tableName));

        return (HTable) table;
    }

    /**
     * 获取一条HBase的作业配置
     * @return
     */
    public static JobConf getHbaseJobConf() {
        JobConf jobConf = new JobConf(conf);
//        jobConf.setOutputFormat(TableOutputFormat.class);

        return jobConf;
    }

    /**
     * 获取HBase的配置类
     * @return
     */
    public static Configuration getConf() {
        return conf;
    }

    /**
     * 获取HBase的连接
     * @return
     */
    public static Connection getConn() {
        return conn;
    }


    /**
     * 生成一个插入HBase的Put实体
     *
     * @param row HBase数据
     * @param columns   HBase列名
     * @param cf    HBase列簇
     * @return
     */
    public static Put createHBasePut(Row row, String[] columns, String cf) {
        String uuid = UUID.randomUUID().toString().replace("-", "");
        Put put = new Put(Bytes.toBytes(uuid));
        for (int i = 0; i < columns.length; i++) {
            if ((null != row.getString(i)) && ("".equals(row.getString(i)) == false)) {
                HBaseUtil.addHBasePutColumn(put, cf, columns[i], row.getString(i));
            }
        }
        return put;
    }

    /**
     * 向HBase的插入数据的实体Put添加Cell
     * Cell的RowKey、列名、值全部为byte[]
     *
     * @param put   HBase插入一条数据的实体
     * @param cf    HBase的列簇
     * @param col   HBase一个Cell对应的字段名
     * @param value HBase一个Cell的值
     * @return
     */
    public static Put addHBasePutColumn(Put put, byte[] cf, byte[] col, byte[] value) {
        put.addColumn(cf, col, value);
        return put;
    }

    /**
     * 向HBase的插入数据的实体Put添加Cell
     * @param put   HBase插入一条数据的实体
     * @param cf    HBase的列簇
     * @param col   HBase一个Cell对应的字段名
     * @param value HBase一个Cell的值
     * @return
     */

    public static Put addHBasePutColumn(Put put, String cf, String col, String value) {
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(col), Bytes.toBytes(value));
        return put;
    }

    /**
     * Spark生成HFile文件并写HBase
     *
     * @param infoRDD SparkRDD， 其所包含的一条数据为HBase的一条数据的一个Cell
     * @param tablename HBase表名
     * @param cf    HBase列簇
     * @param tempHDFSPath  HFile文件临时保存目录，如果已经存在先删除再创建，导入HBase后再删除
     * @throws Exception
     */
    public static void writeData2HBase(JavaPairRDD<RowkeyColumnSecondarySort, String> infoRDD, String tablename, String cf, String tempHDFSPath) throws Exception {
        //将rdd转换成HFile需要的格式,Hfile的key是ImmutableBytesWritable,Value为KeyValue
        JavaPairRDD<ImmutableBytesWritable, KeyValue> hfileRDD = infoRDD.mapToPair(
                new PairFunction<Tuple2<RowkeyColumnSecondarySort, String>, ImmutableBytesWritable, KeyValue>() {
                    @Override
                    public Tuple2<ImmutableBytesWritable, KeyValue> call(Tuple2<RowkeyColumnSecondarySort, String> tuple2) throws Exception {
                        //rowkey
                        String rowkey = tuple2._1().getRowkey();
                        //字段名
                        String column = tuple2._1().getColumn();
                        //字段值
                        String value = tuple2._2();

                        ImmutableBytesWritable im = new ImmutableBytesWritable(Bytes.toBytes(rowkey));
                        KeyValue kv = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));
                        return new Tuple2<>(im, kv);
                    }
                }
        );

        //HDFS路径
        Path path = new Path(tempHDFSPath);

        //判断HDFS上是否存在此路径，如果存在删除此路径
        FileSystem fileSystem = path.getFileSystem(HBaseUtil.getConf());
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }

        //生成HFile文件并保存到临时目录
        hfileRDD.saveAsNewAPIHadoopFile(tempHDFSPath, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, HBaseUtil.getConf());
        //此处运行完成之后,在临时目录会有我们生成的Hfile文件

        //开始导入HBase表
        RegionLocator regionLocator = HBaseUtil.getConn().getRegionLocator(TableName.valueOf(tablename));

        //创建一个hadoop的mapreduce的job
        Job job = Job.getInstance();

        //此处最重要,需要设置文件输出的key,因为我们要生成HFil,所以outkey要用ImmutableBytesWritable
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);

        //输出文件的内容KeyValue
        job.setMapOutputValueClass(KeyValue.class);

        //根据表名获取表
        HTable table = getTable(tablename);

        //配置HFileOutputFormat2的信息
        HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);

        //创建导入Hbase的对象
        LoadIncrementalHFiles bulkLoader = new LoadIncrementalHFiles(conf);

        //正式开始导入
        bulkLoader.doBulkLoad(path, table);

        //删除在HDFS上创建的临时目录
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
        //关闭连接
        IOUtils.closeQuietly(table);
    }

    /**
     * 根据一条数据及字段名和HBase的RowKey生成HBase的一条数据的多个Cell
     * @param row 一条数据的多个字段
     * @param columns 字段名数组
     * @param rowkey HBase的RowKey
     * @return
     */
    public static List<Tuple2<RowkeyColumnSecondarySort, String>> getHFileCellListByRow(Row row, String[] columns, String rowkey) {
        //返回结果集
        List<Tuple2<RowkeyColumnSecondarySort, String>> list = new ArrayList<>();
        for (int j = 0; j < columns.length; j++) {
            Object value = row.get(j);
            if (null != value) {
                RowkeyColumnSecondarySort sort = new RowkeyColumnSecondarySort(rowkey, columns[j]);
                list.add(new Tuple2<>(sort, value.toString()));
            }
        }
        return list;
    }

    /**
     * 根据一条数据及字段名生成HBase的一条数据的多个Cell
     * HBase的RowKey为UUID
     * @param row
     * @param columns
     * @return
     */
    public static List<Tuple2<RowkeyColumnSecondarySort, String>> getHFileCellListByRow(Row row, String[] columns) {
        //HBase数据的rowkey以UUID的格式生成
        String uuid = UUID.randomUUID().toString().replace("-", "");
        return getHFileCellListByRow(row, columns, uuid);
    }

    public static void main(String[] args) throws Exception {
        String rowkey = "151032210000011_1701012159_1701022159_151032210000011_1701012159_1701022159_151032210000011_1701012159_1701022159_151032210000011_1701012159_1701022159_151032210000011_1701012159_1701022159_151032210000011_1701012159_1701022159_151032210000011_1701012159_1701022159_151032210000011_1701012159_1701022159_151032210000011_1701012159_1701022159_151032210000011_1701012159_1701022159_151032210000011_1701012159_1701022159_151032210000011_1701012159_1701022159_151032210000011_1701012159_1701022159_151032210000011_1701012159_1701022159_151032210000011_1701012159_1701022159_151032210000011_1701012159_1701022159_151032210000011_1701012159_1701022159_151032210000011_1701012159_1701022159_151032210000011_1701012159_1701022159_151032210000011_1701012159_1701022159_151032210000011_1701012159_1701022159_151032210000011_1701012159_1701022159_151032210000011_1701012159_1701022159_151032210000011_1701012159_1701022159_151032210000011_1701012159_1701022159_151032210000011_1701012159_1701022159_151032210000011_1701012159_1701022159_151032210000011_1701012159_1701022159_151032210000011_1701012159_1701022159_151032210000011_1701012159_1701022159_";
        Put put = new Put(Bytes.toBytes(rowkey));
        addHBasePutColumn(put, "info", "name", "dsfgdsgdsgdfs");
        Table table = getTable("user");

        table.put(put);
    }
}
