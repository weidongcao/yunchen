package com.rainsoft.hbase.java;

import com.rainsoft.util.java.DateUtils;
import com.rainsoft.util.java.FieldConstant;
import com.rainsoft.util.java.FileUtils;
import com.rainsoft.util.java.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import scala.Tuple2;

import java.io.File;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * Created by Administrator on 2017-05-10.
 */
public class TranstormBcp2HFile {
    public static void main(String[] args) throws Exception {
        //HBase表名
        String tablename = args[0];
        //Hbase表列簇
        String cf = args[1];
        //bcp文件类型
        String[] columns = FieldConstant.HBASE_FIELD_MAP.get(args[2]);
        //bcp文件所在路径
        String bcpPath = args[3];

        System.out.println("导入的表: " + tablename + "\t|列簇:	 " + cf + "\t|数据类型: " + args[2] + "\t|bcp路径: " + bcpPath);
//        importBcpJob("H_REG_CONTENT_HTTP_TMP", "CONTENT_HTTP", 27, "file:///D:\\0WorkSpace\\Develop\\data\\bcp-http");


        //导入HBase
        importBcpJob(tablename, cf, columns, bcpPath);

        //删除源数据
        /*File inpath = new File(bcpPath);
        if (null != inpath) {
            String[] fileList = inpath.list();
            for (String file : fileList) {
                FileUtils.deleteDir(new File(inpath, file));
            }
        }*/
        System.out.println("导入完成时间：>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> " + DateUtils.TIME_FORMAT.format(new Date()) + " <<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
    }

    /**
     * 将BCP文件导入到HBase
     *
     * @param tablename
     * @param cf
     * @param columns
     * @param bcpPath
     * @throws ParseException
     */
    public static void importBcpJob(String tablename, String cf, String[] columns, String bcpPath) throws Exception {

        SparkConf conf = new SparkConf()
                .setAppName("import pcb data into HBase")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> originalRDD = sc.textFile("file:///D:\\0WorkSpace\\Develop\\data\\bcp\\http");

        System.out.println("开始导入时间：>>>>>>>>>>>>>>>>>>>>>>>> " + DateUtils.TIME_FORMAT.format(new Date()) + " <<<<<<<<<<<<<<<<<<<<<<<<<<<<<");

        JavaPairRDD<String, String> tempPairRDD = originalRDD.mapToPair(
                new PairFunction<String, String, String>() {
                    @Override
                    public Tuple2<String, String> call(String s) throws Exception {
                        String uuid = UUID.randomUUID().toString().replace("-", "");
                        return new Tuple2<String, String>(uuid, s);
                    }
                }
        ).sortByKey(true, 1);

        JavaPairRDD<String, Row> bcpRowRDD = tempPairRDD.mapToPair(
                new PairFunction<Tuple2<String, String>, String, Row>() {
                    @Override
                    public Tuple2<String, Row> call(Tuple2<String, String> tuple) throws Exception {
                        return new Tuple2<String, Row>(tuple._1(), RowFactory.create(tuple._2().replace("\\|$\\|", "").split("\\|#\\|")));
                    }
                }
        );

        //过滤
        JavaPairRDD<String, Row> filterFtpRDD = bcpRowRDD.filter(
                new Function<Tuple2<String, Row>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Row> v1) throws Exception {
                        Row row = v1._2();
                        if (row.length() != columns.length) {
                            return false;
                        } else {
                            return true;
                        }
                    }
                }
        );

        JavaPairRDD<ImmutableBytesWritable, KeyValue> sortedHFileRDD = filterFtpRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, ImmutableBytesWritable, KeyValue>() {
                    @Override
                    public Iterable<Tuple2<ImmutableBytesWritable, KeyValue>> call(Tuple2<String, Row> tuple) throws Exception {
                        List list = new ArrayList<Tuple2<byte[], Row>>();

                        Row row = tuple._2();
                        ImmutableBytesWritable im = new ImmutableBytesWritable(Bytes.toBytes(tuple._1()));
                        for (int i = 0; i < row.length(); i++) {
                            if ((null != row.getString(i)) && ("".equals(row.getString(i)) == false)) {
                                KeyValue kv = new KeyValue(Bytes.toBytes(cf), Bytes.toBytes(columns[i]), Bytes.toBytes(row.getString(i)));
                                list.add(new Tuple2<>(im, kv));
                            }
                        }
                        return list;
                    }
                }
        );


        /*//转换为HBase的数据格式
        JavaPairRDD<BigDecimal, Row> hfileRDD = filterFtpRDD.flatMapToPair(
                new PairFlatMapFunction<Row, BigDecimal, Row>() {
                    @Override
                    public Iterable<Tuple2<BigDecimal, Row>> call(Row row) throws Exception {
                        List list = new ArrayList<Tuple2<byte[], Row>>();

                        String uuid = UUID.randomUUID().toString().replace("-", "");
                        for (int i = 0; i < row.length(); i++) {
                            if ((null != row.getString(i)) && ("".equals(row.getString(i)) == false)) {

                                Row info = RowFactory.create(cf, columns[i], row.getString(i));
                                list.add(new Tuple2<>(Bytes.toBigDecimal(Bytes.toBytes(uuid)), info));
                            }
                        }
                        return list;
                    }
                }
        ).sortByKey(true, 1);


        JavaPairRDD<ImmutableBytesWritable, KeyValue> sortedHFileRDD = hfileRDD.mapToPair(
                new PairFunction<Tuple2<BigDecimal, Row>, ImmutableBytesWritable, KeyValue>() {
                    @Override
                    public Tuple2<ImmutableBytesWritable, KeyValue> call(Tuple2<BigDecimal, Row> tuple) throws Exception {
                        ImmutableBytesWritable im = new ImmutableBytesWritable(Bytes.toBytes(tuple._1()));

                        KeyValue kv = new KeyValue(Bytes.toBytes(tuple._2().getString(0)), Bytes.toBytes(tuple._2().getString(1)), Bytes.toBytes(tuple._2().getString(2)));
                        return new Tuple2<>(im, kv);
                    }
                }
        );*/
        /**
         *生成HFile文件
         */
        Configuration hbaseConf = HBaseUtil.getHbaseJobConf();
        String outputPath = "/user/root/hbase/hfile/http/test13";
        sortedHFileRDD.saveAsNewAPIHadoopFile(outputPath, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, hbaseConf);

        /**
         * 导入HBase
         */
        //利用bulk load hfile
        LoadIncrementalHFiles bulkLoader = new LoadIncrementalHFiles(hbaseConf);
        bulkLoader.doBulkLoad(new Path(outputPath), (HTable) HBaseUtil.getTable(tablename));
    }
}
