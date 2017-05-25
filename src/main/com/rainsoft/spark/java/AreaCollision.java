package com.rainsoft.spark.java;

import com.rainsoft.hbase.hfile.java.RowkeyColumnSecondarySort;
import com.rainsoft.util.java.*;
import com.rainsoft.util.scala.BcpUtil;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.codehaus.groovy.runtime.powerassert.SourceText;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.*;

/**
 * Created by Administrator on 2017-05-18.
 */
public class AreaCollision implements Serializable {

    private static final long serialVersionUID = 8356592970426198131L;
    public static String temple_captureTime = " (sn_code in (${machine_ids}) and capture_time > '${start_time}' and capture_time < '${end_time}') ";
    public static String or = "or";

    public static void main(String[] args) throws Exception {


        args[0] = "{ '高嘉花园':['2017-05-19 10:00:00','2017-05-19 11:00:00',['EN1801E116480345','EN1801E116480347']], '文星花园':['2017-05-19 12:00:00','2017-05-19 13:00:00',['EN1801E116480349','EN1801E116480351']], '金鼎大厦':['2017-05-19 14:00:00','2017-05-19 15:00:00',['EN1801E116480353','EN1801E116480375']] }";

        Date start_time = new Date();
        //区域碰撞作业ID
        String jid = UUID.randomUUID().toString().replace("-", "");
        String[] h_collision_result_columns = FieldConstant.HBASE_FIELD_MAP.get("h_collision_result");
        String conditions = args[0];//生成的HFile的临时保存路径
        String hfilePath = "hdfs://dn1.hadoop.com:8020/user/root/hbase/table/h_collision_result/hfile";


        JSONObject conditionJSON = JSONObject.fromObject(conditions);
        JSONObject areaJSON = new JSONObject(false);

        StringBuffer areaConditions = new StringBuffer();

        Iterator iter = conditionJSON.keySet().iterator();

        int areaNum = 0;
        while (iter.hasNext()) {
            if (areaNum > 0) {
                areaConditions.append(or);
            }

            String key = (String) iter.next();
            JSONArray condition = conditionJSON.getJSONArray(key);

            String startTime = condition.optString(0);
            String endTime = condition.optString(1);
            JSONArray macs = condition.optJSONArray(2);

            String capt = temple_captureTime.replace("${start_time}", startTime);
            capt = capt.replace("${end_time}", endTime);
            capt = capt.replace("${machine_ids}", macs.toString().replace("[", "").replace("]", ""));

            areaConditions.append(capt);

            areaJSON.put(key, macs);
            areaNum++;
        }

        InputStream in = AreaCollision.class.getClassLoader().getResourceAsStream("sql/AreaCollision.sql");
        String templeSql = IOUtils.toString(in);
        IOUtils.closeQuietly(in);

        String sql = templeSql.replace("${collision_condition}", areaConditions.toString());

        SparkConf conf = new SparkConf()
                .setAppName("智能分析 区域碰撞")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext sqlContext = new HiveContext(sc.sc());
        sqlContext.sql("use yuncai");

        JavaRDD<Row> macRDD = sqlContext.sql(sql).javaRDD();

        JavaPairRDD<String, Row> imsiPairRDD = macRDD.mapToPair(
                (PairFunction<Row, String, Row>) row -> new Tuple2<>(row.getString(0), row)
        );

        JavaPairRDD<String, Iterable<Row>> imsiGroupPairRDD = imsiPairRDD.groupByKey();
        imsiGroupPairRDD.collect();

        int finalAreaNum = areaNum;
        JavaPairRDD<String, Iterable<Row>> imsiFilterGroupPairRDD = imsiGroupPairRDD.filter(
                (Function<Tuple2<String, Iterable<Row>>, Boolean>) v1 -> {
                    Iterator iter1 = v1._2().iterator();
                    int count = 0;
                    while (iter1.hasNext()) {
                        iter1.next();
                        count++;
                    }
                    if (count < finalAreaNum) {
                        return false;
                    } else {
                        return true;
                    }
                }
        );

        JavaRDD<Row> imsiCountByAreaRDD = imsiFilterGroupPairRDD.map(
                (Function<Tuple2<String, Iterable<Row>>, Row>) v1 -> {
                    String imsi = v1._1();
                    String phone7 = IMSIUtils.getMobileAll(imsi);
                    //按区域分组统计人员出现次数
                    JSONObject areaCountJSON = new JSONObject(false);
                    //获取区域名下的设备
                    Iterator<Row> iter12 = v1._2().iterator();
                    //统计同一区域下的所有设备的人员出现次数
                    while (iter12.hasNext()) {
                        Row row = iter12.next();
                        //设备
                        String machineCode = row.getString(1);
                        //同一设备下人员出现次数
                        int machineAppearTimes = Integer.valueOf(row.get(2).toString());

                        /**
                         * 遍历统计结果JSON
                         * 判断统计结果JSON中是否包含指定区域，
                         * 如果包含出现次数相加
                         * 如果不包含添加此区域
                         */
                        for (Object e : areaJSON.keySet()) {
                            //获取区域
                            String area = e.toString();
                            //判断区域下的设备是否包含此设备号
                            if (areaJSON.getJSONArray(area).contains(machineCode)) {
                                //判断输出结果集是否包含此区域
                                if (areaCountJSON.containsKey(area)) {
                                    //如果包含，则出现次数相加
                                    int sum = areaCountJSON.optInt(area);
                                    areaCountJSON.put(area, sum + machineAppearTimes);
                                } else {
                                    //如果不包含，则添加
                                    areaCountJSON.put(area, machineAppearTimes);
                                }
                            }
                        }
                    }
                    return RowFactory.create(imsi, phone7, areaCountJSON.toString());
                }
        );

        //创建按区域统计结果的元数据
        StructType imsiSchema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("imsi_code", DataTypes.StringType, true),
                DataTypes.createStructField("phone_num", DataTypes.StringType, true),
                DataTypes.createStructField("area_appear_time", DataTypes.StringType, true)
        ));

        //创建按区域统计结果的DataFrame
        DataFrame imsiCountDF = sqlContext.createDataFrame(imsiCountByAreaRDD, imsiSchema);

        //注册为临时表
        imsiCountDF.registerTempTable("imsi_area");

        // 获取区域统计结果注册的临时表与IMSI表及高危人群表进行join的sql
        in = AreaCollision.class.getClassLoader().getResourceAsStream("resource/sql/AreaCollisionJoin.sql");

        // 获取sql
        String finalResultSql = IOUtils.toString(in);

        //关闭文件流
        IOUtils.closeQuietly(in);

        //按区域统计结果注册的临时表与IMSI表及高危人群表进行join，取得最终数据
        DataFrame resultDF = sqlContext.sql(finalResultSql);

        //将查询出来的数据转换成HFile的一个Cell的格式的数据
        JavaPairRDD<RowkeyColumnSecondarySort, String> hfilePairRDD = resultDF.javaRDD().flatMapToPair(
                (PairFlatMapFunction<Row, RowkeyColumnSecondarySort, String>) row -> {
                    //返回结果集
                    List<Tuple2<RowkeyColumnSecondarySort, String>> list = new ArrayList<>();
                    //生成rowkey的波动范围
                    String range = UUID.randomUUID().toString().replace("-", "");
                    //生成rowkey
                    String rowkey = jid + "_" + range;
                    //生成HFile文件的一个Cell的格式的数据
                    for (int i = 0; i < h_collision_result_columns.length; i++) {
                        //rowkey
                        String value = row.get(i).toString();
                        if (null != value) {
                            //生成二次排序实体类
                            RowkeyColumnSecondarySort secondarySortKey = new RowkeyColumnSecondarySort(rowkey, h_collision_result_columns[i]);
                            //添加到返回结果集
                            list.add(new Tuple2<>(secondarySortKey, value));
                        }
                    }
                    return list;
                }
        );

        //对Cell格式的数据进行二次排序，先按rowkey进行排序，如果rowkey相同则再按列名进行排序
        JavaPairRDD<RowkeyColumnSecondarySort, String> sortedhfilePairRDD = hfilePairRDD.sortByKey();


        //生成一条job的数据
        String[] jobInfo = new String[]{DateUtils.TIME_FORMAT.format(start_time), DateUtils.TIME_FORMAT.format(new Date()), "0", conditionJSON.toString(), 0 + ""};
        try {
            //将统计的结果写入HBase
            HBaseUtil.writeData2HBase(sortedhfilePairRDD, "h_collision_result", "field", hfilePath);
            //将job的状态设置为成功
            jobInfo[4] = 1 + "";
        } catch (Exception e) {
            e.printStackTrace();
            //将Job的状态设置为失败
            jobInfo[4] = -1 + "";
        }

        //将job的信息生成HBase的Put类，写入HBase
        Put jobPut = new Put(Bytes.toBytes(jid));
        //区域碰撞表字段
        String[] h_collision_job_columns = FieldConstant.HBASE_FIELD_MAP.get("h_collision_job");

        //生成Put
        for (int i = 0; i < h_collision_result_columns.length; i++) {
            if (null != jobInfo[i]) {
                HBaseUtil.addHBasePutColumn(jobPut, TableConstant.HBASE_CF_FIELD, h_collision_job_columns[i], jobInfo[i]);
            }
        }
        //HBase表名
        Table table = HBaseUtil.getTable("h_collision_job");
        //写入HBase
        table.put(jobPut);

        //关闭连接
        table.close();
        IOUtils.closeQuietly(HBaseUtil.getConn());
    }
}
