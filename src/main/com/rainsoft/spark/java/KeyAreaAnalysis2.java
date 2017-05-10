package com.rainsoft.spark.java;

import com.rainsoft.manager.ConfManager;
import com.rainsoft.util.java.*;
import net.sf.json.JSONArray;
import org.apache.commons.io.FileUtils;
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
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Administrator on 2017-04-24.
 */
public class KeyAreaAnalysis2 {
    public static void main(String[] args) throws IOException, ParseException {
        run(args);
    }

    public static void run(String[] args) throws IOException, ParseException {
        DateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        SparkConf conf = new SparkConf()
                .setAppName("重点小区人群分析")
                .setJars(new String[]{"/opt/caoweidong/software/mysql-connector-java-5.1.38.jar"});
        conf.setMaster(args[0]);

        JavaSparkContext sc = new JavaSparkContext(conf);


        /**
         * 读取设备采集的BCP文件,将设备采集的BCP数据与手机信息表进行join
         * 获取到IMSI号与手机号及手机归属地信息
         */

        String source = args[2];
        JavaRDD<Row> fullInfoRDD;
        //获取SQL所有的文件
        File keyAreaSqlFile = new File("test_emphasisAnalysis.sql");
        //读取SQL
        String keyAreaSql = FileUtils.readFileToString(keyAreaSqlFile);

        //获取统计日期
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(timeFormat.parse(args[1]));

        Date curDate;
        int curMinute = calendar.get(Calendar.MINUTE);
        //如果上次本次统计的结束时间是整小时的话sql语句会把统计结果算下到一个小时,故减1秒钟获取上一个小时
        if (curMinute == 0) {
            curDate = new Date(calendar.getTimeInMillis() - 1000);
        } else {
            curDate = calendar.getTime();
        }
        //当前统计日期
        String count_curTime = timeFormat.format(curDate);
        //替换sql里的时间变量
        keyAreaSql = keyAreaSql.replace("${count_curTime}", count_curTime);


        //获取上次统计日期,后面在判断人员出现是否连续的时候也会用去
        Date preCurDate_temp = new Date(curDate.getTime() - (ConfManager.getInteger(PropConstants.EMPHASIS_TIME_INTERVAL) * 1000));

        //如果上次统计的结束时间是整小时的话sql语句无法到上一个小时统计的结果,故减1秒获取上一个小时的结果结果
        int preMinute = preCurDate_temp.getMinutes();
        Date preCurDate;
        if (preMinute == 0) {
            preCurDate = new Date(preCurDate_temp.getTime() - 1 * 1000);
        } else {
            preCurDate = preCurDate_temp;
        }


        //上次统计的结束时间，格式（yyyy-MM-dd HH:mm:ss）
        String count_preCurTime = timeFormat.format(preCurDate);
        //上次统计的日期，格式（yyyy-MM-dd）
        String count_preCurDate = dateFormat.format(preCurDate);
        //上次统计的小时
        String count_preCurHr = NumberUtils.getFormatInt(2, 2, preCurDate.getHours());

        keyAreaSql = keyAreaSql.replace("${count_preCurTime}", count_preCurTime);
        keyAreaSql = keyAreaSql.replace("${count_preCurDate}", count_preCurDate);
        keyAreaSql = keyAreaSql.replace("${count_preCurHr}", count_preCurHr);

        //恢复当前时间，后面会用到
        calendar.add(Calendar.SECOND, ConfManager.getInteger(PropConstants.EMPHASIS_TIME_INTERVAL));

        if (source.equals("hive")) {

            System.out.println("-----------------------------------------");
            System.out.println("采集信息表与手机信息表join的sql = " + keyAreaSql);
            System.out.println("-----------------------------------------");

            HiveContext sqlContext = new HiveContext(sc.sc());
            //与相关表进行join
            sqlContext.sql("use yuncai");
            fullInfoRDD = sqlContext.sql(keyAreaSql).javaRDD();

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
        } else {
            JavaRDD<String> originalRDD = sc.textFile("file:///D:\\0WorkSpace\\Develop\\data\\fullinfo.txt");
            fullInfoRDD = originalRDD.map(
                    (Function<String, Row>) s -> {
                        String[] arr = s.split("\t");

                        return RowFactory.create(
                                StringUtils.replaceNull(arr[0]),
                                StringUtils.replaceNull(arr[1]),
                                StringUtils.replaceNull(arr[2]),
                                StringUtils.replaceNull(arr[3]),
                                StringUtils.replaceNull(arr[4]),
                                StringUtils.replaceNull(arr[5]),
                                StringUtils.replaceNull(arr[6]),
                                StringUtils.replaceNull(arr[7]),
                                StringUtils.replaceNull(arr[8]),
                                StringUtils.replaceNull(arr[9]),
                                StringUtils.replaceNull(arr[10]),
                                StringUtils.replaceNull(arr[11]),
                                Integer.valueOf(arr[12]),
                                Integer.valueOf(arr[13]),
                                Integer.valueOf(arr[14]),
                                StringUtils.replaceNull(arr[15]),
                                StringUtils.replaceNull(arr[16]),
                                StringUtils.replaceNull(arr[17]),
                                Integer.valueOf(arr[18]),
                                Integer.valueOf(arr[19]),
                                Integer.valueOf(arr[20]),
                                StringUtils.replaceNull(arr[21]),
                                StringUtils.replaceNull(arr[22]),
                                StringUtils.replaceNull(arr[23]),
                                StringUtils.replaceNull(arr[24]),
                                StringUtils.replaceNull(arr[25]),
                                StringUtils.replaceNull(arr[26])
                        );
                    }
            );

        }

        /**
         * 过滤重复的数据
         * 以采集数据的IMSI号、小区号和分析数据的IMSI号和小区号的拼接作为Key
         * 对数据进行groupByKey()
         * 分组后每组只取一条数据
         */
        JavaPairRDD<String, Row> fullInfoPairRDD = fullInfoRDD.mapToPair(
                new PairFunction<Row, String, Row>() {
                    @Override
                    public Tuple2<String, Row> call(Row row) throws Exception {
                        //采集数据IMSI号(0)
                        String curr_imsiCode = row.getString(0);
                        //采集数据小区号(5)
                        String curr_serviceCode = row.getString(5);
                        //分析数据IMSI号(15)
                        String hist_imsiCode = row.getString(15);
                        //分析数据小区号(17)
                        String hist_serviceCode = row.getString(17);

                        String key = curr_imsiCode + "_" + curr_serviceCode + "_" + hist_imsiCode + "_" + hist_serviceCode;
                        return new Tuple2<>(key, row);
                    }
                }
        );

        //对数据进行分组
        JavaPairRDD<String, Iterable<Row>> fullInfoGroupPairRDD = fullInfoPairRDD.groupByKey();

        //对分组数据进行去重
        JavaRDD<Row> distinctFullInfoRDD = fullInfoGroupPairRDD.map(
                (Function<Tuple2<String, Iterable<Row>>, Row>) tuple -> tuple._2().iterator().next()
        );

        JavaRDD<Row> emphasisAnalysisRDD = distinctFullInfoRDD.map(
                (Function<Row, Row>) row -> {
                    //采集数据IMSI号(0)
                    String curr_imsiCode = StringUtils.replaceNull(row.getString(0));
                    //采集数据手机号前7位(1)
                    String curr_phoneNum = StringUtils.replaceNull(row.getString(1));
                    //采集数据手机归属地名(2)
                    String curr_areaName = StringUtils.replaceNull(row.getString(2));
                    //采集数据手机归属地代码(3)
                    String curr_areaCode = StringUtils.replaceNull(row.getString(3));
                    //数据采集时间(4)
                    String curr_captureTime_string = StringUtils.replaceNull(row.getString(4));
                    //采集数据小区号(5)
                    String curr_serviceCode = StringUtils.replaceNull(row.getString(5));
                    //采集数据小区名(6)
                    String curr_serviceName = StringUtils.replaceNull(row.getString(6));
                    //手机运营商(7)
                    String curr_phoneType = StringUtils.replaceNull(row.getString(7));
                    //高危地区(8)
                    String curr_dangerAreaBrief = StringUtils.replaceNull(row.getString(8));
                    //高危人群(9)
                    String curr_dangerPersonBrief = StringUtils.replaceNull(row.getString(9));
                    //高危人群等级(10)
                    String curr_dangerPersonRank = StringUtils.replaceNull(row.getString(10));
                    //高危人群类型(11)
                    String curr_dangerPersonType = StringUtils.replaceNull(row.getString(11));
                    //可疑人群计算周期(12)
                    int curr_doubtfulPeriod = row.getInt(12);
                    //可疑人群出现天数(13)
                    int curr_doubtfulDays = row.getInt(13);
                    //可疑人群出现次数(14)
                    int curr_doubtfulTimes = row.getInt(14);

                    //分析数据IMSI号(15)
                    String hist_imsiCode = StringUtils.replaceNull(row.getString(15));
                    //分析数据小区名(16)
                    String hist_serviceName = StringUtils.replaceNull(row.getString(16));
                    //分析数据小区号(17)
                    String hist_serviceCode = StringUtils.replaceNull(row.getString(17));
                    //分析数据人员出现天数(18)
                    int hist_appearDays = row.getInt(18);
                    //分析数据人员出现次数(19)
                    int hist_appearTimes = row.getInt(19);
                    //分析数据人员类型(20)
                    int hist_peopleType = row.getInt(20);
                    //高危人群类型(21)
                    String hist_danger_personType = StringUtils.replaceNull(row.getString(21));
                    //高危人群等级(22)
                    String hist_danger_personRank = StringUtils.replaceNull(row.getString(22));
                    //高危人群备注(23)
                    String hist_danger_personBrief = StringUtils.replaceNull(row.getString(23));
                    //高危地区备注(24)
                    String hist_dnager_areaBrief = StringUtils.replaceNull(row.getString(24));
                    //分析数据人员标识(25)
                    String hist_identification = StringUtils.replaceNull(row.getString(25));
                    //分析数据最后一次采集时间
                    String hist_lastCaptureTime = StringUtils.replaceNull(row.getString(26));

                    /*
                     * 如果可疑人群判断的周期、天数、次数为空,取默认的
                     */
                    if (curr_doubtfulPeriod == 0) {
                        //可疑人群计算周期
                        curr_doubtfulPeriod = Constants.EMPHASIS_DOUBTFUL_PERIOD;
                        //可疑人群出现天数
                        curr_doubtfulDays = Constants.EMPHASIS_DOUBTFUL_PERIOD;
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
                     * 人群类型有四种：普通人群,可疑人群,高危人群,高危地区人群
                     * 用四位二进制标识表示
                     * 第一位：标识当天是否出现
                     * 第二位：标识高危地区人群
                     * 第三位：标识高危人群
                     * 第四位：标识可疑人群
                     * 如：
                     * 0000表示今天没有出现的普通人群,
                     * 1000表示今天出现的普通人群,
                     * 0001表示今天没有出现的可疑人群,
                     * 1001表示今天出现的可疑人群,
                     * 0010表示今天没有出现的高危人群,
                     * 1010表示今天出现的高危人群,
                     * 0100表示今天没有出现的高危人群,
                     * 1100表示今天出现的高危人群,
                     */
                    String hist_binaryPeopleType = NumberUtils.getFormatInt(4, 4, Integer.valueOf(Integer.toBinaryString(Integer.valueOf(hist_peopleType))));
                    /**
                     *判断人群类型二进制缓存
                     *
                     */
                    StringBuilder curr_binaryPeopleTypeSB = new StringBuilder();

                    //当前出现天数
                    int curr_appearDays = 0;
                    //当前出现次数
                    int curr_appearTimes = 0;
                    if (((hist_imsiCode != null) && (hist_imsiCode.equals("460110674878498"))) || ((curr_imsiCode != null) && (curr_imsiCode.equals("460110674878498")))) {
                        if (((hist_serviceCode != null) && (hist_serviceName.equals("高嘉花园"))) || ((curr_serviceCode != null) && (curr_serviceName.equals("高嘉花园")))) {
                            System.out.println("alskdjfl");
                        }
                    }

                    /*
                     * 判断此次统计是否为新的一天,例如,默认情况下是5分钟统计一下
                     * 现在是00：00,5分钟前是昨天,5分钟后是今天
                     * 如果采集时间为空获取当前时间,为空的话说明采集时间段内是没有出现的,对采集时间段内不影响
                     */
                    //本次采集时间
                    Date currCaptureTime_date = null;
                    if ((curr_captureTime_string != null) && ("".equals(curr_captureTime_string) != true)) {
                        //获取采集时间
                        currCaptureTime_date = timeFormat.parse(curr_captureTime_string);

                        /**
                         * 本次所有的计算都是基于采集时间在心跳时间内
                         */
                        if ((currCaptureTime_date.before(curDate)) && (currCaptureTime_date.after(preCurDate))) {

                        } else {
                            System.out.println("脏数据,采集时间不在统计的心中时间范围内");
                            System.out.println("IMSI -> " + curr_imsiCode + " || service_code -> " + curr_serviceCode);
                            System.exit(-1);
                        }
                    }

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
                            curr_binaryPeopleTypeSB.append(hist_binaryPeopleType.substring(1, 3));

                            /**
                             * 判断指定频率下是否为连续出现
                             * 统计数据记录的最后一次采集时间加上心跳时间，
                             * 如果在本次统计截止时间和上次统计截止时间之间就是连续的
                             */
                            //历史分析数据里最后一次出现的时间
                            Date lastAppearTime = timeFormat.parse(hist_lastCaptureTime);
                            //统计数据记录的最后一次采集时间加上心跳时间
                            Date lastAppearTime_add = new Date(lastAppearTime.getTime() + (ConfManager.getInteger(PropConstants.EMPHASIS_TIME_INTERVAL) * 1000));

                            //判断是否连续:加上心跳时间后在上次统计时间和本次统计时间之间
                            boolean ifContinuationAppear = (lastAppearTime_add.before(curDate)) && (lastAppearTime_add.after(preCurDate));

                            /**
                             * 有两种情况需要重新计算人群类型和标识符
                             * 第一种情况：是新的一天
                             * 第二种情况：不是新的一天，但是分析数据记录的最后采集时间与当前采集时间不在同一天
                             * 此时当天的标识符是以0占位的，需要把0移除再加上今天的标识
                             */
                            if ((curDate.getDate() != preCurDate.getDate())
                                    || (lastAppearTime.getDate() != currCaptureTime_date.getDate())
                                    ) {

                                /**
                                 * 分析数据记录的最后一刻采集时间与当前采集时间不在同一天，
                                 * 且是新的一天，这个时候已经加上了占位符0
                                 * 需要把它移除
                                 */
                                if ((lastAppearTime.getDate() != currCaptureTime_date.getDate())
                                        && (curDate.getDate() == preCurDate.getDate())
                                        ) {
                                    jsonHistIdentification.remove(0);
                                }

                                /**
                                 * 重新统计出现的天数和次数
                                 * 会有三种情况：
                                 * 1：标识符的长度小于统计周期,例如默认情况下是此人此区域是第二天出现,原来的标识符是：[1],现在就要变成[1,1]
                                 * 2：标识符的长度等于统计周期,例如默认情况下此人已经在此出现了3天,今天是第四天出现,就要把四天前的标识移除,
                                 * 3：标识符的长度大于统计周期，例如统计周期为7天，7天前连续出现了3天，然后7天都没有出现,这个时候标识符的长度就是10,
                                 * 然后达到最大排除期限的时候就会把这个人的数据清除掉
                                 */
                                while (jsonHistIdentification.size() >= curr_doubtfulPeriod) {
                                    //移除超期的标识
                                    jsonHistIdentification.remove(jsonHistIdentification.size() - 1);
                                }

                                /**
                                 * 判断标识符内最早的一天是否出现,没有出现的话移除,
                                 * 例如默认情况下此人此区域的出现情况是这样的[1, 0, 1]
                                 * 明天天再出现标识符不能是[1, 1, 0],得是[1, 1]
                                 * 如果此人在此区域出现的是[0, 0, 1]
                                 * 明天再出现标识符不能是[1, 0, 0],得是[1]
                                 */
                                while ((jsonHistIdentification.size() > 0)
                                        && ("0".equals(jsonHistIdentification.optString(jsonHistIdentification.size() - 1)))
                                        ) {
                                    jsonHistIdentification.remove(jsonHistIdentification.size() - 1);
                                }


                                //重新统计天数和次数
                                for (int i = 0; i < jsonHistIdentification.size(); i++) {
                                    //周期内某天出现的次数
                                    int times = jsonHistIdentification.optInt(i);
                                    //次数相加
                                    curr_appearTimes += times;

                                    /**
                                     * 标识符为JSON数组,某天出现的情况会有三种：
                                     * 0表示没有出现
                                     * X表示出现了，但是出现次数为0
                                     * 大于0表示出现多少次
                                     *
                                     * net.sf.json.JSONArray.optInt()方法会自动把X转为0
                                     * net.sf.json.JSONArray.optString()方法会自动把X认识为字符串"X"
                                     */
                                    if ("0".equals(jsonHistIdentification.optString(i)) == false) {
                                        //出现的天数增加
                                        curr_appearDays++;
                                    }
                                }

                                //增加今天出现的天数
                                curr_appearDays++;
                                //增加今天出现的次数
                                curr_appearTimes++;

                                //判断是否是连续出现
                                if (ifContinuationAppear == true) {
                                    // 如果是连续出现算做是出现一次
                                    curr_appearTimes--;

                                    //标识符昨天出现的次数减少一次,把连续出现算到是最近一次出现的里面
                                    //比如说心跳时间是5分钟,昨天出现了两次，最后一次是在23:57,标识符为[2],今天00:02又出现了一次,出现次数还是2次,标识符为[1, 1]
                                    int yesterdayAppearTimes = jsonHistIdentification.optInt(0);

                                    //昨天出现次数减1, 算到今天里
                                    yesterdayAppearTimes--;

                                    /**
                                     * 例如：某人A从晚上9点到第二天2点连续一直出现，
                                     * 程序会认定
                                     * 此人第一天出现为0次，
                                     * 第二天出现了1
                                     *
                                     * 但是第一天也出现了，于是用占位符表示
                                     */
                                    jsonHistIdentification.remove(0);
                                    if (yesterdayAppearTimes == 0) {
                                        jsonHistIdentification.add(0, "X");
                                    } else {
                                        jsonHistIdentification.add(0, yesterdayAppearTimes);
                                    }
                                }


                                //增加今天的标识符
                                jsonHistIdentification.add(0, 1);

                            } else {    //不是新的一天
                                //出现次数加1
                                curr_appearTimes = hist_appearTimes + 1;

                                //当前出现天数取统计数据
                                curr_appearDays = hist_appearDays;

                                //当天出现次数
                                int todayAppearTime = jsonHistIdentification.optInt(0);

                                //判断是否是连续出现
                                if (ifContinuationAppear == true) {
                                    //一共出现的次数减少一次
                                    curr_appearTimes--;
                                    //当天出现的次数不变

                                } else {
                                    //当天出现的次数增加一次
                                    todayAppearTime++;
                                }

                                /**
                                 *标识符变更
                                 */
                                //更新今天出现的数次
                                jsonHistIdentification.remove(0);
                                jsonHistIdentification.add(0, todayAppearTime);
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
                                    ) {     //判断可疑人群,标识为1
                                //标识为可疑人群
                                curr_binaryPeopleTypeSB.append("1");
                            } else {        //判断还是可疑人群标识为0
                                curr_binaryPeopleTypeSB.append("0");
                            }
                        }
                    } else {    //统计周期内当前没有出现的人员
                        //最后一次采集时间取历史数据
                        curr_captureTime_string = hist_lastCaptureTime;
                        //人群类型,当天没有出现的标识标记为0
                        curr_binaryPeopleTypeSB.append("0");
                        //人群类型,高危人群和高危地区人群取历史数据标识
                        curr_binaryPeopleTypeSB.append(hist_binaryPeopleType.substring(1, 4));

                        //人员IMSI号取历史数据
                        curr_imsiCode = hist_imsiCode;

                        if (curDate.getDate() != preCurDate.getDate()) {      //是新的一天
                            if (jsonHistIdentification.size() >= Constants.EMPHASIS_MAX_PERIOD) {
                                jsonHistIdentification.clear();
                            } else {
                                jsonHistIdentification.add(0, 0);
                            }
                        }
                        curr_appearDays = hist_appearDays;
                        curr_appearTimes = hist_appearTimes;

                    }

                    /**
                     * 历史数据不为空取历史数据的信息,当前数据可能为空,也可能不为空,这是不做判断
                     */
                    if (hist_serviceCode != null) {

                        //小区名取历史数据
                        curr_serviceName = hist_serviceName;
                        //小区号取历史数据
                        curr_serviceCode = hist_serviceCode;

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
                            curr_captureTime_string,
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
        );

        //根据标识符过滤需要移除的数据
        JavaRDD<Row> filterEmphasisAnalysisRDD = emphasisAnalysisRDD.filter(
                (Function<Row, Boolean>) row -> {
                    String identification = row.getString(11);
                    JSONArray jsonArray = JSONArray.fromObject(identification);
                    //判断标识符是否为空,如果为空则移除
                    if (jsonArray.size() == 0)
                        return false;
                    else
                        return true;
                }
        );

        String insertType = args[3];
        if (insertType.equals("hbase")) {
            //数据写入HBase
            insertHbase(filterEmphasisAnalysisRDD);
        } else if (insertType.equals("file")) {
            //将数据写入本地文件
            insertFile(filterEmphasisAnalysisRDD);
        } else if (insertType.equals("hive")) {
            //将数据写入Hive
            insertOverHive(filterEmphasisAnalysisRDD, sc, curDate);
        }
    }

    /**
     * 将分析结果写入本地文件
     *
     * @param infoRDD
     * @throws IOException
     */
    public static void insertFile(JavaRDD<Row> infoRDD) throws IOException {
        File emphasisArea = new File("emphasisArea.txt");
        if (emphasisArea.exists()) {
            emphasisArea.delete();
        }
        emphasisArea.createNewFile();

        infoRDD.foreach(
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
                    FileUtils.writeStringToFile(emphasisArea, line + "\n", true);
                }
        );
    }

    /**
     * 将数据HBase
     *
     * @param infoRDD 要写入HBase的数据
     */
    public static void insertHbase(JavaRDD<Row> infoRDD) {
        //将重点区域分析结果转换成Hbase格式的数据
        JavaPairRDD<ImmutableBytesWritable, Put> hbaseEmphasisAnalysisRDD = infoRDD.mapToPair(
                new PairFunction<Row, ImmutableBytesWritable, Put>() {
                    @Override
                    public Tuple2<ImmutableBytesWritable, Put> call(Row row) throws Exception {
                        //Hbase的rowkey以UUID的形式自动生成
                        String uuid = UUID.randomUUID().toString().replace("-", "");
                        //创建Hbase数据
                        Put put = new Put(Bytes.toBytes(uuid));

                        String TEMP_CF_COMMUNITY_ANALYSIS = TableConstants.HBASE_CF;
                        //添加小区名
                        put.addColumn(Bytes.toBytes(TEMP_CF_COMMUNITY_ANALYSIS), Bytes.toBytes("service_name"), Bytes.toBytes(row.getString(0)));
                        //添加小区号
                        put.addColumn(Bytes.toBytes(TEMP_CF_COMMUNITY_ANALYSIS), Bytes.toBytes("service_code"), Bytes.toBytes(row.getString(1)));
                        //人员IMSI号
                        put.addColumn(Bytes.toBytes(TEMP_CF_COMMUNITY_ANALYSIS), Bytes.toBytes("imsi_code"), Bytes.toBytes(row.getString(2)));
                        //添加采集日期
                        put.addColumn(Bytes.toBytes(TEMP_CF_COMMUNITY_ANALYSIS), Bytes.toBytes("last_capture_time"), Bytes.toBytes(row.getString(3)));
                        //指定人员指定小区出现天数
                        put.addColumn(Bytes.toBytes(TEMP_CF_COMMUNITY_ANALYSIS), Bytes.toBytes("appear_days"), Bytes.toBytes(row.getInt(4) + ""));
                        //指定人员指定小区出现次数
                        put.addColumn(Bytes.toBytes(TEMP_CF_COMMUNITY_ANALYSIS), Bytes.toBytes("appear_times"), Bytes.toBytes(row.getInt(5) + ""));
                        //指定人员指定小区的人群类型
                        put.addColumn(Bytes.toBytes(TEMP_CF_COMMUNITY_ANALYSIS), Bytes.toBytes("people_type"), Bytes.toBytes(row.getInt(6) + ""));
                        //高危人群类型
                        if (row.getString(7) != null) {
                            put.addColumn(Bytes.toBytes(TEMP_CF_COMMUNITY_ANALYSIS), Bytes.toBytes("danger_person_type"), Bytes.toBytes(row.getString(7)));
                        }
                        //高危人群排名
                        if (row.getString(8) != null) {
                            put.addColumn(Bytes.toBytes(TEMP_CF_COMMUNITY_ANALYSIS), Bytes.toBytes("danger_person_rank"), Bytes.toBytes(row.getString(8)));
                        }
                        //高危人员说明
                        if (row.getString(9) != null) {
                            put.addColumn(Bytes.toBytes(TEMP_CF_COMMUNITY_ANALYSIS), Bytes.toBytes("danger_person_brief"), Bytes.toBytes(row.getString(9)));
                        }
                        //高危地区说明
                        if (row.getSeq(10) != null) {
                            put.addColumn(Bytes.toBytes(TEMP_CF_COMMUNITY_ANALYSIS), Bytes.toBytes("dnager_area_brief"), Bytes.toBytes(row.getString(10)));
                        }
                        //标识符
                        put.addColumn(Bytes.toBytes(TEMP_CF_COMMUNITY_ANALYSIS), Bytes.toBytes("identification"), Bytes.toBytes(row.getString(11)));

                        return new Tuple2<>(new ImmutableBytesWritable(), put);
                    }
                }
        );

        //获取Hbase的任务配置对象
        JobConf jobConf = HBaseUtil.getHbaseJobConf();
        //设置要插入的HBase表
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, TableConstants.HTABLE_EMPHASIS_ANALYSIS);

        //将数据写入HBase
        hbaseEmphasisAnalysisRDD.saveAsHadoopDataset(jobConf);

    }

    /**
     * 将分析结果写入Hive表中
     */
    public static void insertOverHive(JavaRDD<Row> infoRDD, JavaSparkContext sc, Date curDate) {

        /**
         * 构建DataFrame元数据
         */
        StructType infoSchema = DataTypes.createStructType(Arrays.asList(
                //小区名
                DataTypes.createStructField("service_name", DataTypes.StringType, true),
                //小区号
                DataTypes.createStructField("service_code", DataTypes.StringType, true),
                //IMSI号
                DataTypes.createStructField("imsi_code", DataTypes.StringType, true),
                //采集时间
                DataTypes.createStructField("last_capture_time", DataTypes.StringType, true),
                //出现天数
                DataTypes.createStructField("appear_days", DataTypes.IntegerType, true),
                //出现次数
                DataTypes.createStructField("appear_times", DataTypes.IntegerType, true),
                //人群类型
                DataTypes.createStructField("people_type", DataTypes.IntegerType, true),
                //高危人群类型
                DataTypes.createStructField("danger_person_type", DataTypes.StringType, true),
                //高危人群排名
                DataTypes.createStructField("danger_person_rank", DataTypes.StringType, true),
                //高危人员备注
                DataTypes.createStructField("danger_person_brief", DataTypes.StringType, true),
                //高危地区备注
                DataTypes.createStructField("dnager_area_brief", DataTypes.StringType, true),
                //标识符
                DataTypes.createStructField("identification", DataTypes.StringType, true)
        ));

        //创建HiveContext
        HiveContext sqlContext = new HiveContext(sc.sc());

        //构建SparkSQL临时表
        DataFrame infoDF = sqlContext.createDataFrame(infoRDD, infoSchema);
        infoDF.registerTempTable("info");

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String sql = "insert into buffer_emphasis_analysis partition(ds='%s', hr='%s') select * from info";

        //切换数据库
        sqlContext.sql("use yuncai");
        //先删除之前的数据
        sqlContext.sql(String.format("alter table buffer_emphasis_analysis drop partition(ds='%s', hr='%s')", dateFormat.format(curDate), NumberUtils.getFormatInt(2, 2, curDate.getHours())));
        //插入新数据
        sqlContext.sql(String.format(sql, dateFormat.format(curDate), NumberUtils.getFormatInt(2, 2, curDate.getHours())));
    }
}
