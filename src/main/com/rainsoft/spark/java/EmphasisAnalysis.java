package com.rainsoft.spark.java;

import com.rainsoft.manager.ConfManager;
import com.rainsoft.util.java.Constants;
import com.rainsoft.util.java.PropConstants;
import com.rainsoft.util.java.IMSIUtils;
import com.rainsoft.util.java.TableConstants;
import net.sf.json.JSONArray;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by CaoWeidong on 2017-04-24.
 */
public class EmphasisAnalysis {
    public static void main(String[] args) throws IOException, ParseException {

        DateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SparkConf conf = new SparkConf()
                .setAppName("重点小区人群分析")
                .setJars(new String[]{"/opt/caoweidong/software/mysql-connector-java-5.1.38.jar"});

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new HiveContext(sc.sc());

        String type = args[1];
        String path = args[0];

        //二进制字符串格式比如3要转换为三位二进制为11，前面要补0
        NumberFormat numFormat = NumberFormat.getInstance();
        //设置是否使用分组
        numFormat.setGroupingUsed(false);
        //设置最大整数位数
        numFormat.setMaximumIntegerDigits(4);
        //设置最小整数位数
        numFormat.setMinimumIntegerDigits(4);

        /**
         * 读取设备采集的BCP文件，将设备采集的BCP数据与手机信息表进行join
         * 获取到IMSI号与手机号及手机归属地信息
         */
        JavaRDD<String> improveDataRDD = sc.textFile("file:///" + path);

        /*
         * 通过IMSI号生成手机号前7位，并注册为临时表
         */
        //生成IMSI临时表数据
        JavaRDD<String> filterImproveDataRDD = improveDataRDD.filter(
                (Function<String, Boolean>) s -> {
                    String[] arr = s.split("\\|#\\|");
                    if ((arr != null) && (arr.length == 14)) {
                        return true;
                    } else {
                        return false;
                    }
                }
        );
        JavaRDD<Row> improveRowRDD = filterImproveDataRDD.map(
                (Function<String, Row>) s -> {
                    s = s.replace("\\|$\\|", "");
                    String[] arr = s.split("\\|#\\|");
                    String phone7 = IMSIUtils.getMobileAll(arr[1]);
                    return RowFactory.create(arr[0], arr[1], phone7, arr[2], arr[3], arr[4]);
                }
        );

        //生成IMSI临时表元数据
        StructType improveSchema = DataTypes.createStructType(Arrays.asList(
                //手机号前7位的表头
                DataTypes.createStructField("equipment_mac", DataTypes.StringType, true),
                DataTypes.createStructField("imsi_code", DataTypes.StringType, true),
                DataTypes.createStructField("phone_num", DataTypes.StringType, true),
                DataTypes.createStructField("capture_time", DataTypes.StringType, true),
                DataTypes.createStructField("operator_type", DataTypes.StringType, true),
                DataTypes.createStructField("sn_code", DataTypes.StringType, true)
        ));

        //创建DataFrame
        DataFrame imsiDF = sqlContext.createDataFrame(improveRowRDD, improveSchema);

        //注册为区域采集数据临时表
        imsiDF.registerTempTable("improve");


        /**
         *IMSI及手机信息表与相关表进行join获取到数据分析需要的信息
         * 相关的表有：
         * 1.手机信息表(h_sys_phone_to_area)
         * 2.高危人群表(h_ser_rq_danger_person)
         * 3.高危地区表(h_ser_rq_danger_area)
         * 4.设备表(h_machine_info)
         * 5.小区信息表(h_service_info)
         * 6.重点区域配置表(h_ser_rq_emphasis_area_config)
         * 7.历史数据分析表
         */
        //获取SQL所有的文件
        File keyAreaSqlFile = new File("test_emphasisAnalysis.sql");
        //读取SQL
        String keyAreaSql = FileUtils.readFileToString(keyAreaSqlFile);

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(timeFormat.parse("2017-05-03 16:40:01"));
        calendar.add(Calendar.SECOND, -ConfManager.getInteger(PropConstants.EMPHASIS_TIME_INTERVAL));

        String preCountDate = dateFormat.format(calendar.getTime());
        keyAreaSql.replace("${preCountDate}", preCountDate);

        System.out.println("-----------------------------------------");
        System.out.println("采集信息表与手机信息表join的sql = " + keyAreaSql);
        System.out.println("-----------------------------------------");

        //与相关表进行join
        sqlContext.sql("use yuncai");
        JavaRDD<Row> fullInfoRDD = sqlContext.sql(keyAreaSql).javaRDD();

        File fullinfo = new File("fullinfo.txt");
        if (fullinfo.exists()) {
            fullinfo.delete();
        }
        fullinfo.createNewFile();

        fullInfoRDD.foreach(
                (VoidFunction<Row>) row -> {
                    String line = row.getString(0) + "\t"
                            + row.getString(1) + "\t"
                            + row.getString(2) + "\t"
                            + row.getString(3) + "\t"
                            + row.getString(4) + "\t"
                            + row.getString(5) + "\t"
                            + row.getString(6) + "\t"
                            + row.getString(7) + "\t"
                            + row.getString(8) + "\t"
                            + row.getString(9) + "\t"
                            + row.getString(10) + "\t"
                            + row.getString(11) + "\t"
                            + row.getInt(12) + "\t"
                            + row.getInt(13) + "\t"
                            + row.getInt(14) + "\t"
                            + row.getString(15) + "\t"
                            + row.getString(16) + "\t"
                            + row.getString(17) + "\t"
                            + row.getInt(18) + "\t"
                            + row.getInt(19) + "\t"
                            + row.getInt(20) + "\t"
                            + row.getString(21) + "\t"
                            + row.getString(22) + "\t"
                            + row.getString(23) + "\t"
                            + row.getString(24) + "\t"
                            + row.getString(25) + "\t"
                            + row.getString(26);
                    FileUtils.writeStringToFile(fullinfo, line + "\n", true);
                }
        );

        JavaRDD<Row> emphasisAnalysisRDD = fullInfoRDD.map(
                new Function<Row, Row>() {
                    @Override
                    public Row call(Row row) throws Exception {
                        //采集数据IMSI号(0)
                        String curr_imsiCode = row.getString(0);
                        //采集数据手机号前7位(1)
                        String curr_phoneNum = row.getString(1);
                        //采集数据手机归属地名(2)
                        String curr_areaName = row.getString(2);
                        //采集数据手机归属地代码(3)
                        String curr_areaCode = row.getString(3);
                        //数据采集时间(4)
                        String curr_captureTime = row.getString(4);
                        //采集数据小区号(5)
                        String curr_serviceCode = row.getString(5);
                        //采集数据小区名(6)
                        String curr_serviceName = row.getString(6);
                        //手机运营商(7)
                        String curr_phoneType = row.getString(7);
                        //高危地区(8)
                        String curr_dangerAreaBrief = row.getString(8);
                        //高危人群(9)
                        String curr_dangerPersonBrief = row.getString(9);
                        //高危人群等级(10)
                        String curr_dangerPersonRank = row.getString(10);
                        //高危人群类型(11)
                        String curr_dangerPersonType = row.getString(11);
                        //可疑人群计算周期(12)
                        int curr_doubtfulPeriod = row.getInt(12);
                        //可疑人群出现天数(13)
                        int curr_doubtfulDays = row.getInt(13);
                        //可疑人群出现次数(14)
                        int curr_doubtfulTimes = row.getInt(14);

                        //分析数据IMSI号(15)
                        String hist_imsiCode = row.getString(15);
                        //分析数据小区名(16)
                        String hist_serviceName = row.getString(16);
                        //分析数据小区号(17)
                        String hist_serviceCode = row.getString(17);
                        //分析数据人员出现天数(18)
                        int hist_appearDays = row.getInt(18);
                        //分析数据人员出现次数(19)
                        int hist_appearTimes = row.getInt(19);
                        //分析数据人员类型(20)
                        int hist_peopleType = row.getInt(20);
                        //高危人群类型(21)
                        String hist_danger_personType = row.getString(21);
                        //高危人群等级(22)
                        String hist_danger_personRank = row.getString(22);
                        //高危人群备注(23)
                        String hist_danger_personBrief = row.getString(23);
                        //高危地区备注(24)
                        String hist_dnager_areaBrief = row.getString(24);
                        //分析数据人员标识(25)
                        String hist_identification = row.getString(25);
                        //分析数据最后一次采集时间
                        String hist_hdate = row.getString(26);



                        /*
                         * 如果可疑人群判断的周期、天数、次数为空，取默认的
                         */
                        if (curr_doubtfulPeriod == 0) {
                            //可疑人群计算周期
                            curr_doubtfulPeriod = Constants.EMPHASIS_DOUBTFUL_PERIOD;
                            //可疑人群出现天数
                            curr_doubtfulDays = Constants.EMPHASIS_DOUBTFUL_DAYS;
                            //可疑人群出现次数
                            curr_doubtfulTimes = Constants.EMPHASIS_DOUBTFUL_TIMES;
                        }
                        //将标识符转换为JSON
                        if (hist_identification == null) {
                            hist_identification = "[]";
                        }
                        JSONArray jsonHistIdentification = JSONArray.fromObject(hist_identification);

                        /**
                         * 判断人群类型
                         * 人群类型有四种：普通人群，可疑人群，高危人群，高危地区人群
                         * 用四位二进制标识表示
                         * 第一位：标识当天是否出现
                         * 第二位：标识高危地区人群
                         * 第三位：标识高危人群
                         * 第四位：标识可疑人群
                         * 如：
                         * 0000表示今天没有出现的普通人群，
                         * 1000表示今天出现的普通人群，
                         * 0001表示今天没有出现的可疑人群，
                         * 1001表示今天出现的可疑人群，
                         * 0010表示今天没有出现的高危人群，
                         * 1010表示今天出现的高危人群，
                         * 0100表示今天没有出现的高危人群，
                         * 1100表示今天出现的高危人群，
                         */
                        String hist_binaryPeopleType = numFormat.format(Integer.valueOf(Integer.toBinaryString(Integer.valueOf(hist_peopleType))));
                        /**
                         *判断人群类型二进制缓存
                         *
                         */
                        StringBuilder curr_binaryPeopleTypeSB = new StringBuilder();

                        //当前出现天数
                        int curr_appearDays = 0;
                        //当前出现次数
                        int curr_appearTimes = 0;

                        /*
                         * 判断此次统计是否为新的一天，例如，默认情况下是5分钟统计一下
                         * 现在是00：00，5分钟前是昨天，5分钟后是今天
                         * 如果采集时间为空获取当前时间，为空的话说明采集时间段内是没有出现的，对采集时间段内不影响
                         */
                        Calendar cale = Calendar.getInstance();
                        Date curDate = new Date();
                        if ((curr_captureTime != null) && ("".equals(curr_captureTime) != true)) {
                            //获取采集时间
                            curDate = timeFormat.parse(curr_captureTime);
                        }
                        //获取采集时间是哪一天
                        cale.setTime(curDate);

                        int curday = cale.get(Calendar.DATE);
                        //获取上一次的采集时间
                        cale.add(Calendar.SECOND, ConfManager.getInteger(PropConstants.EMPHASIS_TIME_INTERVAL));
                        //获取上一次采集时间是哪一天
                        int preCurday = cale.get(Calendar.DATE);


                        /**
                         * 业务逻辑：
                         * 统计周期内当前出现的人员
                         *     是新出现的人员
                         *
                         *     不是新出现的人员
                         *         是新的一天
                         *             更新出现的天数
                         *             更新出现的次数
                         *             更新人群类型
                         *             更新关注类型
                         *             更新标识符
                         *         不是新的一天
                         * 统计周期内当前没有出现的人员
                         *
                         */
                        if (curr_imsiCode != null) {    //统计周期内当前出现的人员
                            //当天出现的标识符设为1
                            curr_binaryPeopleTypeSB.append("1");

                            if (hist_imsiCode == null) {    //是新出现的人员
                                //出现1天
                                curr_appearDays = 1;
                                //出现1次
                                curr_appearTimes = 1;


                                //判断高危地区
                                if (curr_dangerAreaBrief != null) {
                                    curr_binaryPeopleTypeSB.append("1");
                                } else {
                                    curr_binaryPeopleTypeSB.append("0");
                                }
                                //判断高危人员
                                if (curr_dangerPersonBrief != null) {
                                    curr_binaryPeopleTypeSB.append("1");
                                } else {
                                    curr_binaryPeopleTypeSB.append("0");
                                }

                                //第一次出现肯定不是可疑人员
                                curr_binaryPeopleTypeSB.append("0");

                                //标识符
                                jsonHistIdentification.clear();
                                jsonHistIdentification.add(1);

                            } else {    //不是新出现的人员

                                //高危人群、高危地区人群取历史数据
                                curr_binaryPeopleTypeSB.append(hist_binaryPeopleType.substring(1, 2));

                                //判断指定频率下是否为连续出现
                                //历史分析数据里最后一次出现的时间
                                Date lastAppearTime = timeFormat.parse(hist_hdate);
                                //当前出现的时间与历史分析数据里最后一次出现的时间做比较获取时间差（秒）
                                long diffSecond = (curDate.getTime() - lastAppearTime.getTime()) / (1000);

                                //判断是否是指定时间间隔连续出现，比如采集时间间隔是5分钟,之前最后一次出现是在2017-05-03 16:35, 如果2017-05-03 16:40再次出现就是连续出现
                                //如果是连续出现算做是出现一次
                                if (diffSecond <= ConfManager.getInteger(PropConstants.EMPHASIS_TIME_INTERVAL)) {
                                    curr_appearTimes--;
                                }

                                if (curday != preCurday) {  //是新的一天
                                    /**
                                     * 重新统计出现的天数和次数
                                     * 会有两种情况：
                                     * 1：标识符的长度小于统计周期，例如默认情况下是此人此区域是第二天出现,原来的标识符是：[1],现在就要变成[1,1]
                                     * 2：标识符的长度等于统计周期,例如默认情况下此人已经在此出现了3天，今天是第四天出现，就要把四天前的标识移除，
                                     */
                                    while (jsonHistIdentification.size() >= curr_doubtfulPeriod) {
                                        //移除超期的标识
                                        jsonHistIdentification.remove(jsonHistIdentification.size() - 1);
                                    }
                                    /**
                                     * 判断周期内最早的一天是否出现，没有出现的话移除，
                                     * 例如默认情况下此人此区域的出现情况是这样的[1, 0, 1]
                                     * 明天天再出现标识符不能是[1, 1, 0],得是[1, 1]
                                     * 如果此人在此区域出现的是[0, 0, 1]
                                     * 明天再出现标识符不能是[1, 0, 0],得是[1]
                                     */
                                    while (jsonHistIdentification.optInt(jsonHistIdentification.size() - 1) == 0) {
                                        jsonHistIdentification.remove(jsonHistIdentification.size() - 1);
                                    }

                                    //增加今天的标识符
                                    jsonHistIdentification.add(0, 1);

                                    //重新统计天数和次数
                                    for (int i = 0; i < jsonHistIdentification.size(); i++) {
                                        //周期内某天出现的次数
                                        int times = jsonHistIdentification.optInt(i);
                                        //次数相加
                                        curr_appearTimes += times;
                                        //判断出现的天数
                                        curr_appearDays++;
                                    }
                                } else {    //不是新的一天
                                    //出现次数加1
                                    curr_appearTimes = hist_appearTimes + 1;
                                    //出现天数不变
                                    curr_appearDays = hist_appearDays;

                                    /**
                                     *标识符变更
                                     */
                                    //更新今天出现的数次
                                    jsonHistIdentification.remove(0);
                                    jsonHistIdentification.add(0, curr_appearTimes);
                                }

                                /**
                                 * 判断可疑人群
                                 * 判断条件分为两组四个条件
                                 * 第一组：
                                 *      1.判断是否要按天数进行判断
                                 *      2.如果按天数进行判断的话是否大于等于指定天数
                                 * 第二组：
                                 *      1.判断是否要按次数进行判断
                                 *      2.如果按次数进行判断的话是否大于等于指定次数
                                 */
                                if (((curr_doubtfulDays != 0) && (curr_appearDays >= curr_doubtfulDays))
                                        || ((curr_doubtfulTimes != 0) && (curr_appearTimes >= curr_doubtfulTimes))
                                        ) {     //判断可疑人群，标识为1
                                    //标识为可疑人群
                                    curr_binaryPeopleTypeSB.append("1");
                                } else {        //判断还是可疑人群标识为0
                                    curr_binaryPeopleTypeSB.append("0");
                                }
                            }
                        } else {    //统计周期内当前没有出现的人员
                            //人群类型，当天没有出现的标识标记为0
                            curr_binaryPeopleTypeSB.append("0");
                            //人群类型，高危人群和高危地区人群取标识历史数据
                            curr_binaryPeopleTypeSB.append(hist_binaryPeopleType.substring(1, 3));

                            //人员IMSI号取历史数据
                            curr_imsiCode = hist_imsiCode;

                            if (curday != preCurday) {      //是新的一天
                                if (jsonHistIdentification.size() >= Constants.EMPHASIS_MAX_PERIOD) {
                                    jsonHistIdentification.clear();
                                } else {
                                    jsonHistIdentification.add(0);
                                }
                            }
                            //当前出现天数取历史分析数据出现天数
                            curr_appearDays = 0;
                            //当天出现次数取历史分析数据出现次数
                            curr_appearTimes = 0;
                            //可疑人群标识位不做判断
                            curr_binaryPeopleTypeSB.append('0');

                        }

                        /**
                         * 历史数据不为空取历史数据的信息，当前数据可能为空，也可能不为空，这是不做判断
                         */
                        if (hist_serviceCode != null) {

                            //小区名取历史数据
                            curr_serviceName = hist_serviceName;
                            //小区号取历史数据
                            curr_serviceCode = hist_serviceCode;
                            //最后一次采集时间取历史数据
                            curr_captureTime = hist_hdate;

                            //高危人群类型取历史数据
                            curr_dangerPersonType = hist_danger_personType;
                            //高危人群等级取历史数据
                            curr_dangerPersonRank = hist_danger_personRank;
                            //高危人群备注取历史数据
                            curr_dangerPersonBrief = hist_danger_personBrief;
                            //高危地区取类型取历史数据
                            curr_dangerAreaBrief = hist_dnager_areaBrief;
                        }


                        return RowFactory.create(
                                curr_serviceName,
                                curr_serviceCode,
                                curr_imsiCode,
                                curr_captureTime,
                                curr_appearDays,
                                curr_appearTimes,
                                Integer.valueOf(curr_binaryPeopleTypeSB.toString(), 2),
                                curr_dangerPersonType,
                                curr_dangerPersonRank,
                                curr_dangerPersonBrief,
                                curr_dangerAreaBrief,
                                jsonHistIdentification.toString()
                        );
                    }
                }
        );

        //根据标识符过滤需要移除的数据
        JavaRDD<Row> filterEmphasisAnalysisRDD = emphasisAnalysisRDD.filter(
                (Function<Row, Boolean>) row -> {
                    String identification = row.getString(11);
                    JSONArray jsonArray = JSONArray.fromObject(identification);
                    //判断标识符是否为空，如果为空则移除
                    if (jsonArray.size() == 0)
                        return false;
                    else
                        return true;
                }
        );

        //将重点区域分析结果转换成Hbase格式的数据
        JavaPairRDD<ImmutableBytesWritable, Put> hbaseEmphasisAnalysisRDD = filterEmphasisAnalysisRDD.mapToPair(
                new PairFunction<Row, ImmutableBytesWritable, Put>() {
                    @Override
                    public Tuple2<ImmutableBytesWritable, Put> call(Row row) throws Exception {
                        //Hbase的rowkey以UUID的形式自动生成
                        String uuid = UUID.randomUUID().toString().replace("-", "");
                        //创建Hbase数据
                        Put put = new Put(Bytes.toBytes(uuid));

                        String TEMP_CF_COMMUNITY_ANALYSIS = ConfManager.getProperty(TableConstants.HBASE_CF);
                        //添加小区名
                        put.addColumn(Bytes.toBytes(TEMP_CF_COMMUNITY_ANALYSIS), Bytes.toBytes("service_name"), Bytes.toBytes(row.getString(0)));
                        //添加小区号
                        put.addColumn(Bytes.toBytes(TEMP_CF_COMMUNITY_ANALYSIS), Bytes.toBytes("service_code"), Bytes.toBytes(row.getString(1)));
                        //添加采集日期
                        put.addColumn(Bytes.toBytes(TEMP_CF_COMMUNITY_ANALYSIS), Bytes.toBytes("hdate"), Bytes.toBytes(row.getString(2)));
                        //人员IMSI号
                        put.addColumn(Bytes.toBytes(TEMP_CF_COMMUNITY_ANALYSIS), Bytes.toBytes("imsi_code"), Bytes.toBytes(row.getInt(3) + ""));
                        //指定人员指定小区出现天数
                        put.addColumn(Bytes.toBytes(TEMP_CF_COMMUNITY_ANALYSIS), Bytes.toBytes("appear_days"), Bytes.toBytes(row.getInt(4) + ""));
                        //指定人员指定小区出现次数
                        put.addColumn(Bytes.toBytes(TEMP_CF_COMMUNITY_ANALYSIS), Bytes.toBytes("appear_times"), Bytes.toBytes(row.getInt(5) + ""));
                        //指定人员指定小区的人群类型
                        put.addColumn(Bytes.toBytes(TEMP_CF_COMMUNITY_ANALYSIS), Bytes.toBytes("people_type"), Bytes.toBytes(row.getInt(6) + ""));
                        //高危人群类型
                        put.addColumn(Bytes.toBytes(TEMP_CF_COMMUNITY_ANALYSIS), Bytes.toBytes("danger_person_type"), Bytes.toBytes(row.getInt(7) + ""));
                        //高危人群排名
                        put.addColumn(Bytes.toBytes(TEMP_CF_COMMUNITY_ANALYSIS), Bytes.toBytes("danger_person_rank"), Bytes.toBytes(row.getInt(8) + ""));
                        //高危人员说明
                        put.addColumn(Bytes.toBytes(TEMP_CF_COMMUNITY_ANALYSIS), Bytes.toBytes("danger_person_brief"), Bytes.toBytes(row.getInt(9) + ""));
                        //高危地区说明
                        put.addColumn(Bytes.toBytes(TEMP_CF_COMMUNITY_ANALYSIS), Bytes.toBytes("dnager_area_brief"), Bytes.toBytes(row.getInt(10) + ""));
                        //标识符
                        put.addColumn(Bytes.toBytes(TEMP_CF_COMMUNITY_ANALYSIS), Bytes.toBytes("identification"), Bytes.toBytes(row.getInt(11) + ""));

                        return new Tuple2<>(new ImmutableBytesWritable(), put);
                    }
                }
        );

        //获取Hbase的任务配置对象
//            JobConf jobConf = HBaseUtil.getHbaseJobConf();
        //设置要插入的HBase表
//            jobConf.set(TableOutputFormat.OUTPUT_TABLE, ConfManager.getProperty(PropConstants.HTABLE_EMPHASIS_ANALYSIS));

        //将数据写入HBase
//            hbaseEmphasisAnalysisRDD.saveAsHadoopDataset(jobConf);


        File emphasisArea = new File("emphasisArea.txt");
        if (emphasisArea.exists()) {
            emphasisArea.delete();
        }
        emphasisArea.createNewFile();

        filterEmphasisAnalysisRDD.foreach(
                (VoidFunction<Row>) row -> {
                    String line = row.getString(0) + "\t\t"
                            + row.getString(1) + "\t\t"
                            + row.getString(2) + "\t\t"
                            + row.getString(3) + "\t\t"
                            + row.getInt(4) + "\t\t"
                            + row.getInt(5) + "\t\t"
                            + row.getInt(6) + "\t\t"
                            + row.getString(7) + "\t\t"
                            + row.getString(8) + "\t\t"
                            + row.getString(9) + "\t\t"
                            + row.getString(10) + "\t\t"
                            + row.getString(11);
                    System.out.println(line);
                    FileUtils.writeStringToFile(emphasisArea, line + "\n", true);
                }
        );
    }
}
