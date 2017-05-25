package com.rainsoft.oracle2hbase;

import com.rainsoft.hbase.hfile.java.RowkeyColumnSecondarySort;
import com.rainsoft.util.java.Constants;
import com.rainsoft.util.java.FieldConstant;
import com.rainsoft.util.java.HBaseUtil;
import com.rainsoft.util.java.IMSIUtils;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Administrator on 2017-05-24.
 */
public class HSerRqDangerPersonTest {
    public static void main(String[] args) throws Exception {
        List<String> imsis = FileUtils.readLines(new File("D:\\0WorkSpace\\Develop\\data\\count_imsi_improve.txt"));
        List<String> personids = FileUtils.readLines(new File("D:\\0WorkSpace\\Develop\\data\\danger_person_id.txt"));

        List<String[]> list = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            String imsi = imsis.get(i).split(" ")[0];
            String phone7 = IMSIUtils.getMobileAll(imsi);
            String personid = personids.get(i);
            String[] row = new String[]{personid, i % 2 + 1 + "", 0 + "", "brief", "id_no", imsi, phone7, "name", "create_time", "update_time", "create_person"};
            list.add(row);
        }

        SparkConf conf = new SparkConf()
                .setAppName("高危人群信息表数据从Oracle迁移到HBase")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String[]> aaaRDD = sc.parallelize(list);

        JavaPairRDD<RowkeyColumnSecondarySort, String> aaRDD = aaaRDD.flatMapToPair(
                new PairFlatMapFunction<String[], RowkeyColumnSecondarySort, String>() {
                    @Override
                    public Iterable<Tuple2<RowkeyColumnSecondarySort, String>> call(String[] strings) throws Exception {
                        String rowkey = strings[0];
                        Row row = RowFactory.create(Arrays.asList(strings).subList(1, strings.length).toArray());
                        return HBaseUtil.getHFileCellListByRow(row, FieldConstant.HBASE_FIELD_MAP.get("h_ser_rq_danger_person"), rowkey);
                    }
                }
        ).sortByKey();
        HBaseUtil.writeData2HBase(aaRDD, "h_ser_rq_danger_person", "field", Constants.HFILE_TEMP_STORE_PATH + "h_ser_rq_danger_person");




    }
}
