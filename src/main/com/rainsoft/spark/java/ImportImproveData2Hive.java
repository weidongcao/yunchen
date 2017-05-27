package com.rainsoft.spark.java;

import com.rainsoft.manager.ConfManager;
import com.rainsoft.util.java.*;
import org.apache.commons.io.*;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by Administrator on 2017-05-26.
 */
public class ImportImproveData2Hive {
    public static void main(String[] args) throws Exception {
        String path = ConfManager.getProperty(PropConstants.DIR_BCP_RESOURCE);
//        String path = "D:\\0WorkSpace\\Develop\\data\\bcp\\improve\\596463021-1495683656-81459-scan_ending_improve.bcp";
        String improveTableName = TableConstant.HBASE_TABLE_H_SCAN_ENDING_IMPROVE;
        String[] hScanEndingImproveCols = FieldConstant.HBASE_FIELD_MAP.get(improveTableName.toLowerCase());

        String hfilePath = Constants.HFILE_TEMP_STORE_PATH + improveTableName;
        SparkConf conf = new SparkConf()
                .setAppName(ImportImproveData2Hive.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> datasRDD = sc.textFile("file://" + path);

        JavaRDD<Row> rowRDD = datasRDD.map(
                new Function<String, Row>() {
                    @Override
                    public Row call(String v1) throws Exception {
                        String[] fields = v1.replace("\\|$\\|", "").split("\\|#\\|");
                        //IMSI号
                        String imsiCode = fields[1];
                        //导入时间
                        String importTime = DateUtils.TIME_FORMAT.format(new Date());
                        String phone_area = "";
                        //手机号前7位
                        String phoneNum = IMSIUtils.getMobileAll(imsiCode);
                        //将手机号前7位加入到数组
                        //截取需要入库的字段
                        String[] subFields = ArrayUtils.subarray(fields, 0, hScanEndingImproveCols.length - 3);
                        //添加手机号前7位的字段
                        String[] fieldsNew = ArrayUtils.addAll(subFields, new String[]{importTime, phone_area, phoneNum});
                        return RowFactory.create(fieldsNew);
                    }
                }
        );

        JavaRDD<Row> filterRowRDD = rowRDD.filter(
                new Function<Row, Boolean>() {
                    @Override
                    public Boolean call(Row v1) throws Exception {
                        if (v1.size() < hScanEndingImproveCols.length) {
                            return false;
                        } else {
                            return true;
                        }
                    }
                }
        );

//        filterRowRDD.cache();
        //数据写入Hive
        List<StructField> schemaFieldList = new ArrayList<>();
        for (int i = 0; i < hScanEndingImproveCols.length; i++) {
            schemaFieldList.add(DataTypes.createStructField(hScanEndingImproveCols[i], DataTypes.StringType, true));
        }
        StructType schemaImprove = DataTypes.createStructType(schemaFieldList);

        /*File file = new File("improve_hive.txt");
        if (file.exists()) {
            file.exists();
        }
        file.createNewFile();
        filterRowRDD.foreach(
                new VoidFunction<Row>() {
                    @Override
                    public void call(Row row) throws Exception {
                        org.apache.commons.io.FileUtils.writeStringToFile(file, row.mkString("\t") + "\r\n", true);
                    }
                }
        );*/

        HiveContext sqlContext = new HiveContext(sc.sc());
        sqlContext.sql("use yuncai");
        DataFrame improveDF = sqlContext.createDataFrame(filterRowRDD, schemaImprove);

        String tablename = "temp_buffer_improve";
        improveDF.registerTempTable("temp_buffer_improve");

        //hive开启动态分区插入
        sqlContext.sql("set hive.exec.dynamic.partition=true");
        sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict");
        sqlContext.sql("set hive.exec.max.dynamic.partitions.pernode=1000");

        InputStream streamImportImproveData2Hive = ImportImproveData2Hive.class.getClassLoader().getResourceAsStream("sql/ImportImproveData2Hive.sql");
        String templeSql = IOUtils.toString(streamImportImproveData2Hive);
        IOUtils.closeQuietly(streamImportImproveData2Hive);

        String sql = templeSql.replace("${tablename}", tablename);
        sqlContext.sql(sql);


        /*JavaPairRDD<RowkeyColumnSecondarySort, String> hfileRDD = filterRowRDD.flatMapToPair(
                new PairFlatMapFunction<Row, RowkeyColumnSecondarySort, String>() {
                    @Override
                    public Iterable<Tuple2<RowkeyColumnSecondarySort, String>> call(Row row) throws Exception {
                        List<Tuple2<RowkeyColumnSecondarySort, String>> list = new ArrayList<>();
                        String captureTime = row.get(2)
                                .toString()
                                .replace(" ", "")
                                .replace("-", "")
                                .replace(":", "");
                        String machineID = row.get(4).toString();
                        String imsiCode = row.get(1).toString();
                        String suffix = RandomStringUtils.randomAlphanumeric(5);

                        String rowKey = captureTime + "_" + machineID + "_" + imsiCode + suffix;

                        for (int i = 0; i < hScanEndingImproveCols.length; i++) {
                            list.add(new Tuple2<>(new RowkeyColumnSecondarySort(rowKey, hScanEndingImproveCols[i]), row.get(i).toString()));
                        }
                        return list;
                    }
                }
        ).sortByKey();

       HBaseUtil.writeData2HBase(hfileRDD, improveTableName, "ENDING_IMPROVE", hfilePath);*/
    }
}