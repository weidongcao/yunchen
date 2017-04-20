package com.rainsoft.spark.java;

import com.rainsoft.util.java.HBaseUtil;
import com.rainsoft.util.java.IMSIUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

/**
 * 功能说明：通过IMSI号生成IMSI号的手机号前7位以及地区和运营商
 * Created by Administrator on 2017-04-20.
 */
public class CreateIMSIKeyArea {

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf()
                .setAppName("通过IMSI号生成IMSI号的手机号前7位以及地区和运营商")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<String> imsiRDD = sc.textFile("file:///D:\\0WorkSpace\\Develop\\data\\distinct_imsi_20170308_to_20170419");

        JavaRDD<String> phoneAreaRDD = sc.textFile("file:///D:\\0WorkSpace\\Develop\\data\\phone_to_area_30w");

        JavaRDD<Row> imsiRowRDD = imsiRDD.map(
                new Function<String, Row>() {
                    @Override
                    public Row call(String s) throws Exception {
                        String phone7 = IMSIUtils.getMobileAll(s);
                        return RowFactory.create(phone7, s);
                    }
                }
        );

        StructType imsiSchema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("phone7", DataTypes.StringType, true),
                DataTypes.createStructField("imsi", DataTypes.StringType, true)
        ));

        DataFrame imsiDF = sqlContext.createDataFrame(imsiRowRDD, imsiSchema);

        imsiDF.registerTempTable("imsiTable");

        JavaRDD<Row> phoneAreaRowRDD = phoneAreaRDD.map(
                (Function<String, Row>) s -> {
                    String[] arr = s.split("\t");
                    return RowFactory.create(arr[0], arr[1], arr[2], arr[3], arr[4]);
                }
        );

        StructType phoneSchema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("phone7", DataTypes.StringType, true),
                DataTypes.createStructField("area_name", DataTypes.StringType, true),
                DataTypes.createStructField("area_code", DataTypes.StringType, true),
                DataTypes.createStructField("phone_type", DataTypes.StringType, true),
                DataTypes.createStructField("region", DataTypes.StringType, true)
        ));

        DataFrame phoneDF = sqlContext.createDataFrame(phoneAreaRowRDD, phoneSchema);

        phoneDF.registerTempTable("phoneTable");

        String sql = "select i.imsi, p.phone7, p.area_name, p.area_code, p.phone_type, p.region from imsiTable i left join phoneTable p on i.phone7 = p.phone7";
        JavaRDD<Row> infoRDD = sqlContext.sql(sql).javaRDD();

        JavaPairRDD<ImmutableBytesWritable, Put> hbaseIMSIRDD = infoRDD.mapToPair(
                new PairFunction<Row, ImmutableBytesWritable, Put>() {
                    @Override
                    public Tuple2<ImmutableBytesWritable, Put> call(Row row) throws Exception {
                        String uuid = UUID.randomUUID().toString().replace("-", "");
                        Put put = new Put(Bytes.toBytes(uuid));
                        String imsi = row.getString(0);
                        String phone7 = row.getString(1);
                        String areaName = row.getString(2);
                        String areaCode = row.getString(3);
                        String phoneType = row.getString(4);
                        String region = row.getString(5);
                        put.addColumn(Bytes.toBytes("IMSI_KEY_AREA"), Bytes.toBytes("imsi_num"), Bytes.toBytes(imsi));
                        if (phone7 != null) {
                            put.addColumn(Bytes.toBytes("IMSI_KEY_AREA"), Bytes.toBytes("phone_num"), Bytes.toBytes(phone7));
                        }
                        if (areaName != null) {
                            put.addColumn(Bytes.toBytes("IMSI_KEY_AREA"), Bytes.toBytes("area_name"), Bytes.toBytes(areaName));
                        }
                        if (areaCode != null) {
                            put.addColumn(Bytes.toBytes("IMSI_KEY_AREA"), Bytes.toBytes("area_code"), Bytes.toBytes(areaCode));
                        }
                        if (phoneType != null) {
                            put.addColumn(Bytes.toBytes("IMSI_KEY_AREA"), Bytes.toBytes("phone_type"), Bytes.toBytes(phoneType));
                        }
                        if (region != null) {
                            put.addColumn(Bytes.toBytes("IMSI_KEY_AREA"), Bytes.toBytes("region"), Bytes.toBytes(region));
                        }

                        return new Tuple2<>(new ImmutableBytesWritable(), put);
                    }
                }
        );

        JobConf jobConf = HBaseUtil.getHbaseJobConf();
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, "H_SYS_IMSI_KEY_AREA");
        hbaseIMSIRDD.saveAsHadoopDataset(jobConf);
    }
}
