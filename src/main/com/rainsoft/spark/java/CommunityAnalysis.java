package com.rainsoft.spark.java;

import com.rainsoft.dao.ICommunityConfigDao;
import com.rainsoft.dao.factory.DaoFactory;
import com.rainsoft.domain.java.CommunityConfig;
import com.rainsoft.hbase.hfile.java.RowkeyColumnSecondarySort;
import com.rainsoft.util.java.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.json.JSONObject;
import scala.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 说明(默认配置)：
 * 1、实有人口：指昨日出现过的IMSI总数；
 * 2、关注人群：指实有人口中在关注人群库中存在的IMSI数；
 * 3、高危地区人群：指实有人口中归属地在高危地区库中存在的IMSI数；
 * 4、异常常住人群：指10日内未出现过的常住人群；
 * 5、常住人群：昨日出现过的IMSI中，连续出现过7天（含昨天）及以上 或 之前已被标识为常住人群（以最后一次系统或人工标记为准） 的IMSI数；
 * 6、暂住人群：昨日出现过的IMSI中，连续出现过3－6天（含昨天）、且在之前的3个月内未出现过的IMSI数；
 * 7、闪现人群：昨日出现过的IMSI中，连续出现过1－2天（含昨天）、且在之前的3个月内未出现过的IMSI数；
 * 8、其他人群
 * <p>
 * 此外常住人群、暂住人群、闪现人群天数可配置
 */
public class CommunityAnalysis implements Serializable {


    private static final long serialVersionUID = 944884281502914598L;

    //昨天出现的权重
    public static Float WEIGHT_YESTERDAY = Constants.WEIGHT_YESTERDAY;
    //增加的权重
    public static Float WEIGHT_ADD = Constants.WEIGHT_ADD;
    //减少的权重
    public static Float WEIGHT_REDUCE = Constants.WEIGHT_REDUCE;

    public static void main(String[] args) throws Exception {
        String paramDate = args[0];
        int days = Integer.valueOf(args[1]);
        handle(paramDate, days);
    }

    /**
     * @param startDate 开始统计的日期
     * @param days      统计多少天的数据
     * @throws IOException
     * @throws ParseException
     */
    public static void handle(String startDate, int days) throws Exception {

        //获取系统当前日期
        Calendar cale = Calendar.getInstance();

        //保存在模板文件中的SQL
        String originSql;

        //获取要执行的Hive SQL
        InputStream streamCommunityAnalysis = CommunityAnalysis.class.getClassLoader().getResourceAsStream("sql/communityAnalysis.sql");
        //读取文件
        originSql = IOUtils.toString(streamCommunityAnalysis);
        //关闭文件IO
        IOUtils.closeQuietly(streamCommunityAnalysis);

        for (int i = 0; i < days; i++) {
            //测试数据
            Date tmp = DateUtils.DATE_FORMAT.parse(startDate);
            cale.setTime(tmp);
            //昨天日期
            cale.add(Calendar.DATE, -1 + i);
            Date yesterday = cale.getTime();

            System.out.println("---------------当前分析的日期为：" + DateUtils.DATE_FORMAT.format(yesterday) + "---------------");
            //前天日期
            cale.add(Calendar.DATE, -1);
            Date beforeYesterday = cale.getTime();

            //替换昨天的日期
            String hiveSql = originSql.replace("${yesterday}", DateUtils.DATE_FORMAT.format(yesterday));
            //替换前天的日期
            hiveSql = hiveSql.replace("${before_yesterday}", DateUtils.DATE_FORMAT.format(beforeYesterday));

            /**
             * Spark执行Hive sql并返回JavaRDD
             * 返回数据格式：<昨天数据人群IMSI号, 历史数据人员IMSI号, 昨天数据小区名, 昨天数据小区ID, 历史数据小区ID, 昨天数据设备ID, 历史数据人群权重>
             */
            //创建Spark配置对象
            SparkConf conf = new SparkConf()
                    .setAppName("小区人群分析");//Spark应用名


            //创建SparkContext实例
            JavaSparkContext sc = new JavaSparkContext(conf);

            //创建SQLContext
            HiveContext sqlContext = new HiveContext(sc.sc());

            //生产代码
            sqlContext.sql("use yuncai");
            JavaRDD<Row> comprehensiveDataRDD = sqlContext.sql(hiveSql).javaRDD();



            /*//测试代码
            JavaRDD<String> originalRDD = sc.textFile("file:///D:\\0WorkSpace\\Develop\\data\\comprehensive_20170310.txt");

            //测试代码
            JavaRDD<Row> comprehensiveDataRDD = originalRDD.map(
                    new Function<String, Row>() {
                        @Override
                        public Row call(String s) throws Exception {
                            String[] str = s.split("\t");
                            String temp = StringUtils.replaceNull(str[8]);
                            Float weight = (null == temp) ? null : Float.valueOf(temp);
                            return RowFactory.create(
                                    StringUtils.replaceNull(str[0]),
                                    StringUtils.replaceNull(str[1]),
                                    StringUtils.replaceNull(str[2]),
                                    StringUtils.replaceNull(str[3]),
                                    StringUtils.replaceNull(str[4]),
                                    StringUtils.replaceNull(str[5]),
                                    StringUtils.replaceNull(str[6]),
                                    StringUtils.replaceNull(str[7]),
                                    weight,
                                    StringUtils.replaceNull(str[9]),
                                    StringUtils.replaceNull(str[10]),
                                    StringUtils.replaceNull(str[11]),
                                    StringUtils.replaceNull(str[12]),
                                    StringUtils.replaceNull(str[13])
                            );
                        }
                    }
            );*/

            /*File infoFile = new File("comprehensive_20170310.txt");
            if (infoFile.exists()) {
                infoFile.delete();
            }
            infoFile.createNewFile();
            comprehensiveDataRDD.foreach(
                    new VoidFunction<Row>() {
                        @Override
                        public void call(Row row) throws Exception {
                            String str = row.mkString("\t");
                            FileUtils.writeStringToFile(infoFile, str + "\t", true);
                        }
                    }
            );*/

            /**
             * 分析小区人群
             */
            JavaRDD<Row> handlePeopleRDD = comprehensiveDataRDD.map(
                    new Function<Row, Row>() {
                        @Override
                        public Row call(Row row) throws Exception {
                            //昨天数据人群IMSI号
                            String yesterdayIMSI = row.getString(0);
                            //历史数据人员IMSI号
                            String historyIMSI = row.getString(1);
                            //昨天数据小区名
                            String yesterdayServiceName = row.getString(2);
                            //历史数据小区名
                            String historyServiceName = row.getString(3);
                            //昨天数据小区ID
                            String yesterdayServiceCode = row.getString(4);
                            //历史数据小区ID
                            String historyServiceCode = row.getString(5);
                            //昨天数据高危人群信息(6)
                            String yesterdayDangerPersionId = row.getString(6);
                            //昨天数据高危地区信息(7)
                            String yesterdayDangerAreaBrief = row.getString(7);
                            //历史数据人员权重
                            Float historyWeight = row.getFloat(8);
                            //昨天数据手机号前7位
                            String yesterdayPhone7 = row.getString(9);
                            //昨天数据手机归属地(10)
                            String yesterdayAreaName = row.getString(10);
                            //昨天数据归属地代码(11)
                            String yesterdayAreaCode = row.getString(11);
                            //昨天数据手机运营商
                            String yesterdayPhoneType = row.getString(12);
                            //昨天数据手机号码归属地电话区号
                            String yesterdayRegion = row.getString(13);


                            //昨天数据人员权重
                            Float yesterdayWeight = null;
                            //昨天的日期
                            String hdate = DateUtils.DATE_FORMAT.format(yesterday);

                            /*
                             *根据小区ID获取小区配置信息
                             */
                            ICommunityConfigDao communityConfigDao = DaoFactory.getCommunityConfigDao();

                            //小区ID，如果历史数据为空则获取昨天的数据
                            String code = historyServiceCode == null ? yesterdayServiceCode : historyServiceCode;
                            //获取小区配置信息
                            CommunityConfig conf = communityConfigDao.getConfigByServiceCode(code);


                            /**
                             * 判断判断此人在此小区是否是3个月内第一次出现
                             */
                            if (historyWeight == 0) {   //历史的数据里没有此人的权重
                                if (null != yesterdayIMSI) {//昨天的数据里有此人的数据

                                    //加上昨天的权重
                                    yesterdayWeight = WEIGHT_YESTERDAY;


                                    //加上新增的权重
                                    yesterdayWeight = yesterdayWeight + WEIGHT_ADD;
                                }

                            } else {//历史数据里有此人的权重
                                yesterdayWeight = historyWeight;

                                if (null == yesterdayIMSI) {    //昨天新增的数据没有此人在此小区的信息

                                    //将历史数据中人员的IMSI号赋给昨天的数据
                                    yesterdayIMSI = historyIMSI;

                                    //将历史数据中人员所在的小区名赋给昨天的数据
                                    yesterdayServiceName = historyServiceName;

                                    //将历史数据中人员所在的小区ID赋给昨天的数据
                                    yesterdayServiceCode = historyServiceCode;

                                    if ((historyWeight > 0)     //权重为0
                                            && (historyWeight < (conf.getStayCalcDays() * WEIGHT_ADD))  //最大暂停天数乘以增加的权重
                                            ) {     //其他人群：前天此人没有在此小区出现过(历史数据的权重在0到最大暂停天数乘以增加的权重)
                                        /**
                                         * 判断时间范围是否在3个月内
                                         */
                                        //向上取整，保留小数点后2位
                                        Float addWeight = NumberUtils.getFormatDouble(historyWeight, 2, "up");
                                        //没有出现的天数
                                        int reduceDay = (int) ((addWeight - yesterdayWeight) / WEIGHT_REDUCE);

                                        //如果出现的天数小于最大统计天数,则减小权重
                                        if (reduceDay < Constants.COUNT_DAYS) {
                                            yesterdayWeight = historyWeight - WEIGHT_REDUCE;
                                        } else {    //如果出现的天数大于等于最大统计天数则置0，后面把它过滤掉
                                            yesterdayWeight = 0f;
                                        }
                                    } else if ((historyWeight > WEIGHT_YESTERDAY)
                                            && (historyWeight <= (WEIGHT_YESTERDAY + conf.getStayCalcDays() * WEIGHT_ADD))
                                            ) {   //闪现和暂住人群
                                        //减去昨天的权重
                                        yesterdayWeight = historyWeight - WEIGHT_YESTERDAY;
                                        //减去减少的权重
                                        yesterdayWeight -= WEIGHT_REDUCE;
                                    } else if ((historyWeight > (WEIGHT_YESTERDAY + conf.getStayCalcDays() * WEIGHT_ADD))
                                            ) { //常住人群
                                        //减去减少的权重
                                        yesterdayWeight -= WEIGHT_REDUCE;

                                        /**
                                         * 常住人群不出现后再次出现,然后又不出现的情况的情况
                                         * 比如说常住群众A，3天不出现，又出现3天，然后又不出现，此时的情况
                                         * 此时把A出现时增加的权重减去
                                         *
                                         * 这样默认情况下，
                                         * 常住人群昨天没有出现的权重在106-107之间，
                                         * 常住人群昨天出现且连续出现7天及以上的权重都是107，
                                         * 常住人群昨天出现且连续出现小于7天的权重在107-115之间
                                         */
                                        boolean flat = yesterdayWeight > (WEIGHT_YESTERDAY + conf.getLongCalcDays() * WEIGHT_ADD);

                                        //减去不连续出现增加的权重直到权重小于107
                                        while (flat) {
                                            //减去增加的权重
                                            yesterdayWeight -= WEIGHT_ADD;
                                            //判断是否小于107(默认情况下)
                                            flat = yesterdayWeight > (conf.getLongCalcDays() * WEIGHT_ADD);
                                        }

                                        //常住人群超过90天没有出现的权重默认
                                        Float regularDelWeight = WEIGHT_YESTERDAY + conf.getLongCalcDays() * WEIGHT_ADD - Constants.COUNT_DAYS * WEIGHT_REDUCE;
                                        //超过90天没有出现移出
                                        if ((yesterdayWeight <= regularDelWeight)   //昨天数据的权重小于常住人群超过90天没有出现的权重
                                                && (yesterdayWeight > (WEIGHT_YESTERDAY + conf.getStayCalcDays() * WEIGHT_ADD))     //暂住人群的最大权重
                                                ) {//常住人群不再统计的情况：小于常住人群超过90天没有出现的权重(超过90天不再统计),大于暂住人群的最大权重
                                            yesterdayWeight = 0f;
                                        }

                                    }

                                } else {    //昨天新增的数据有此人在此小区的信息
                                    if ((historyWeight > 0)     //权重为0
                                            && (historyWeight < (conf.getStayCalcDays() * WEIGHT_ADD))  //最大暂住天数乘以增加的权重
                                            ) {//其他人群：前天此人没有在此小区出现过(历史数据的权重在0到最大暂停天数乘以增加的权重)
                                        /**
                                         * 其他人群如果连续出现7天会再次转化为常住人群
                                         * 根据其他人群权重的小数位判断其他人群之前是否连续出现
                                         * 如果之前连续出现则其小数位为0
                                         * 如果之前还是连续出现则其小数位有减少的痕迹
                                         */
                                        //获取其他人群出现的次数
                                        int tmp = historyWeight.intValue();

                                        //判断之前的权重是否有减少的痕迹,
                                        if (historyWeight - tmp == 0) {     //如果没有减少的痕迹说明之前连续出现的，之前的权重继续增加
                                            yesterdayWeight = historyWeight + WEIGHT_ADD;
                                        } else {    //如果有说明其之前不是连续出现的，之前的权重舍弃不用
                                            yesterdayWeight = WEIGHT_ADD;
                                        }

                                        //如果其他人群已连接出现7天，则转换为常住人群
                                        if (yesterdayWeight == 7) {
                                            //将权重转为常住人群的正常权重,默认情况下是107
                                            yesterdayWeight = WEIGHT_YESTERDAY + conf.getLongCalcDays() * WEIGHT_ADD;
                                        }

                                    } else if ((historyWeight > WEIGHT_YESTERDAY)
                                            && (historyWeight < (WEIGHT_YESTERDAY + conf.getLongCalcDays() * WEIGHT_ADD))
                                            ) {   //闪现和暂住人群
                                        yesterdayWeight = historyWeight + WEIGHT_ADD;
                                    } else if ((historyWeight > (WEIGHT_YESTERDAY + conf.getStayCalcDays() * WEIGHT_ADD))
                                            ) { //常住人群

                                        //常住人群，被系统判断为常住人群后没有再出现的人员例如权重在10.6-10.7之间
                                        if ((historyWeight > (WEIGHT_YESTERDAY + conf.getStayCalcDays() * WEIGHT_ADD)) &&
                                                (historyWeight < (WEIGHT_YESTERDAY + conf.getLongCalcDays() * WEIGHT_ADD))) {
                                            //权重减少
                                            yesterdayWeight = historyWeight + WEIGHT_ADD;
                                        } else if (historyWeight == (WEIGHT_YESTERDAY + conf.getLongCalcDays() * WEIGHT_ADD)) {
                                            //权重不变
                                            yesterdayWeight = historyWeight;
                                        } else if ((historyWeight > (WEIGHT_YESTERDAY + conf.getLongCalcDays() * WEIGHT_ADD))
                                                && (historyWeight < (WEIGHT_YESTERDAY + 2 * conf.getLongCalcDays() * WEIGHT_ADD))
                                                ) {     //此情况是常住人口，但是最近一段时间没有出现，但是昨天出现了的情况
                                            //权重增加
                                            yesterdayWeight = historyWeight + WEIGHT_ADD;

                                            //判断是否是连续7天增加，例如：（一个常住人口，90天内，前20天没有出现，然后连接出现7天，重置为常住人口，以前增加和减少的权重全都忽略）
                                            if (yesterdayWeight >= (WEIGHT_YESTERDAY + 2 * conf.getLongCalcDays() * WEIGHT_ADD)) {
                                                //重置为常住人口，以前和减少的权重全都忽略
                                                yesterdayWeight = WEIGHT_YESTERDAY + conf.getLongCalcDays() * WEIGHT_ADD;
                                            }
                                        }
                                    }
                                }
                            }

                            /**
                             * 返回分析后的小区人员信息
                             * 格式为org.apache.spark.sql.Row
                             * yesterdayIMSI IMSI号 index(0)
                             * yesterdayServiceName 小区名 index(1)
                             * yesterdayServiceCode 小区Code index(2)
                             * hdate 采集日期 index(3)
                             * yesterdayWeight  权重 index(4)
                             * yesterdayPhone7  手机号前7位(5)
                             * yesterdayAreaName    昨日数据手机号归属地(6)
                             * yesterdayAreaCode    昨日数据手机号归属地ID(7)
                             * yesterdayDangerPersionBrief     昨日数据高危人群ID(8)
                             * yesterdayDangerAreaBrief      昨日数据高危地区地区名(9)
                             */
                            return RowFactory.create(
                                    yesterdayIMSI,
                                    yesterdayServiceName,
                                    yesterdayServiceCode,
                                    hdate,
                                    yesterdayWeight,
                                    yesterdayPhone7,
                                    yesterdayAreaName,
                                    yesterdayAreaCode,
                                    yesterdayDangerPersionId,
                                    yesterdayDangerAreaBrief
                            );
                        }
                    }
            );

            /**
             * 过滤掉超过最大统计天数(默认为90天)没有出现的小区人员
             */
            JavaRDD<Row> filterHandlePeopleRDD = handlePeopleRDD.filter(
                    (Function<Row, Boolean>) row -> {
                        Float weight = row.getFloat(4);
                        if (0 == weight) {
                            return false;
                        } else {
                            return true;
                        }
                    }
            );

            //缓存数据
            filterHandlePeopleRDD.cache();

            String[] h_persion_type_columns = FieldConstant.HBASE_FIELD_MAP.get(TableConstant.HBASE_TABLE_H_PERSION_TYPE);
            /**
             * 分析结果要写入HBase的数据
             * service_name
             * service_code
             * imsi
             * weight
             * hdate
             *
             */
            JavaRDD<Row> persionTypeRDD = filterHandlePeopleRDD.map(
                    (Function<Row, Row>) row -> RowFactory.create(row.getString(1), row.getString(2), row.getString(0), row.getFloat(4), row.getString(3))
            );
            /**
             * 将小区人员分析结果数据写入HBase
             */
            JavaPairRDD<RowkeyColumnSecondarySort, String> hfilRDD = persionTypeRDD.flatMapToPair(
                    new PairFlatMapFunction<Row, RowkeyColumnSecondarySort, String>() {
                        @Override
                        public Iterable<Tuple2<RowkeyColumnSecondarySort, String>> call(Row row) throws Exception {
                            /**
                             * HBase的RowKey以统计日期，小区号，IMSI号以及一个8位随机数后缀组成
                             */
                            String imsi = row.get(2).toString();
                            String serviceCode = row.get(1).toString();
                            String hdate = row.get(4).toString().replace("-", "");
                            String suffix = RandomStringUtils.randomAlphanumeric(8);

                            String rowkey = hdate + "_" + serviceCode + "_" + imsi + "_" + suffix;

                            return HBaseUtil.getHFileCellListByRow(row, FieldConstant.HBASE_FIELD_MAP.get(TableConstant.HBASE_TABLE_H_PERSION_TYPE), rowkey);
                        }
                    }
            ).sortByKey();

            //HDFS上HFile的临时存储目录
            String persionTypeHfileStoreHDFSTempPath = Constants.HFILE_TEMP_STORE_PATH + TableConstant.HBASE_TABLE_H_PERSION_TYPE + "/hfile";
            //写入HBase
            HBaseUtil.writeData2HBase(hfilRDD, TableConstant.HBASE_TABLE_H_PERSION_TYPE, TableConstant.HBASE_CF_FIELD, persionTypeHfileStoreHDFSTempPath);

            /**
             * 统计小区人群
             */
            //以小区ID为主键，转换为<key, value>的形式
            JavaPairRDD<String, Row> communityPairRDD = filterHandlePeopleRDD.mapToPair((PairFunction<Row, String, Row>) row -> new Tuple2<>(row.getString(2), row));

            //根据小区进行分组
            JavaPairRDD<String, Iterable<Row>> communityPeopleTypeRDD = communityPairRDD.groupByKey();

            //统计出昨天各个小区的人群分类情况
            JavaRDD<Row> communityAnalysisRDD = communityPeopleTypeRDD.map(
                    new Function<Tuple2<String, Iterable<Row>>, Row>() {
                        @Override
                        public Row call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                            //小区ID
                            String serviceCode = tuple._1();
                            //小区名
                            String serviceName = "";
                            //统计常住人群
                            int regularCount = 0;
                            //统计暂住人群
                            int temporaryCount = 0;
                            //统计闪现人群
                            int seldomCount = 0;
                            //统计其他人群
                            int otherCount = 0;
                            //统计日期
                            String hdate = DateUtils.DATE_FORMAT.format(yesterday);
                            //统计高危人群
                            int dangerPersionCount = 0;

                            //统计高危地区人数
                            Map<String, Integer> countDangerAreaMap = new HashMap<>();

                            //对同一小区下的数据进行遍历，统计出各类人群
                            for (Row row : tuple._2()) {
                                //小区人员权重
                                Float weight = row.getFloat(4);
                                //小区名
                                serviceName = row.getString(1);

                                //高危地区名
                                String dangerAreaName = row.getString(9);
                                //高危人员
                                String dangerPersion = row.getString(8);

                                /*
                                 * 统计高危人员,如果包含在高危人群库即认为其为高危人群
                                 */
                                if ((dangerPersion != null) && ("".equals(dangerPersion) != true)) {
                                    dangerPersionCount++;
                                }

                                /*
                                 * 统计高危地区人员
                                 */
                                if ((dangerAreaName != null) && ("".equals(dangerAreaName) != true)) {
                                    //如果Map中已经有了此高危地区的信息，加1
                                    if (countDangerAreaMap.containsKey(dangerAreaName)) {
                                        //从Map中获取此高危地区人员的数量
                                        int dangerAreaCount = countDangerAreaMap.get(dangerAreaName);
                                        //数量加1
                                        dangerAreaCount++;
                                        //存放入Map
                                        countDangerAreaMap.put(dangerAreaName, dangerAreaCount);
                                    } else {    //如果Map中没有此高危地区的信息,添加并初始化数量为1
                                        countDangerAreaMap.put(dangerAreaName, 1);
                                    }
                                }

                                //获取小区配置信息DAO
                                ICommunityConfigDao communityConfigDao = DaoFactory.getCommunityConfigDao();

                                //获取小区配置信息实体
                                CommunityConfig conf = communityConfigDao.getConfigByServiceCode(serviceCode);

                                //统计出小区下各类人群数量
                                if ((weight > 0)    //权重大于0
                                        && (weight < (conf.getStayCalcDays() * WEIGHT_ADD))     //权重小于最大暂住天数乘以增加的权重值，默认配置下为6 * 1
                                        ) {     //其他人群，默认配置下权重为（0-6）

                                    //其他人群数量加1
                                    otherCount++;

                                } else if ((weight > WEIGHT_YESTERDAY)      //闪现人群权重大于昨天的权重(1)
                                        && (weight <= (WEIGHT_YESTERDAY + conf.getNewCalcDays() * WEIGHT_ADD))      //闪现人群权重小于等于昨天的权重加上最大闪现人群天数乘以增加的权重，默认配置下为1 + 2 * 0.1
                                        ) {     //闪现人群，权重在昨天的权重与昨天的权重加上最大闪现天数乘以增加的权重，默认配置下权重为(1-1.2]

                                    //闪现人群数量加1
                                    seldomCount++;

                                } else if ((weight > (WEIGHT_YESTERDAY + conf.getNewCalcDays() * WEIGHT_ADD))   //大于最大闪现人群的权重
                                        && (weight <= (WEIGHT_YESTERDAY + conf.getStayCalcDays()))      //小于等于最大暂住人群的权重(昨天的权重加上最大暂住天数乘以增加的权重，默认配置下为1 + 6 * 0.1)
                                        ) {     //暂住人群权重范围：大于最大闪现人群的权重，小于等于最大暂住人群的权重

                                    //暂住人群数量加1
                                    temporaryCount++;

                                } else if ((weight > (WEIGHT_YESTERDAY + conf.getStayCalcDays() * WEIGHT_ADD))      //大于小套暂住人群的权重
                                        && (weight <= (WEIGHT_YESTERDAY + 2 * conf.getLongCalcDays() * WEIGHT_ADD))     //小于等于最大常住人群的权重
                                        ) {     //常住人群权重范围：大于最大暂住人群的权重，小于等于最大常住人群的权重(昨天的权重加上 2 乘以最大常住人群天数再乘以增加的权重,默认配置下为 1 + 2 * 7 * 0.1)

                                    //常住人群数量加1
                                    regularCount++;
                                }
                            }

                            //将高危地区人群统计转为JSON格式
                            JSONObject jsonDangerAreaCount = new JSONObject(countDangerAreaMap);

                            /**
                             * 返回小区人群分析数据
                             * 格式为org.apache.spark.sql.Row
                             * 数据包含(小区名, 小区ID, 采集日期, 常住人群数量, 暂住人群数量, 闪现人群数量, 其他人群数量)
                             * serviceName 小区名  index(0)
                             * serviceCode 小区ID  index(1)
                             * hdate 采集日期  index(2)
                             * regularCount 常住人群数量  index(3)
                             * temporaryCount 暂住人群数量  index(4)
                             * seldomCount 闪现人群数量  index(5)
                             * otherCount 其他人群数量  index(6)
                             * dangerPersionCount 高危人群数量    index(7)
                             * jsonDangerAreaCount.toString()   高危地区人群数量    index(8)
                             */
                            return RowFactory.create(serviceName, serviceCode, regularCount, temporaryCount, seldomCount, otherCount, dangerPersionCount, jsonDangerAreaCount.toString(), hdate);
                        }
                    }
            );

            //小区人群统计表字段
            String[] h_community_analysis_columns = FieldConstant.HBASE_FIELD_MAP.get("h_community_analysis");
            //将小区分析结果转换成Hbase格式的数据
            JavaPairRDD<RowkeyColumnSecondarySort, String> hfielRDD = communityAnalysisRDD.flatMapToPair(
                    new PairFlatMapFunction<Row, RowkeyColumnSecondarySort, String>() {
                        @Override
                        public Iterable<Tuple2<RowkeyColumnSecondarySort, String>> call(Row row) throws Exception {
                            /**
                             * RowKey以统计日期，小区号，以及8位随机数后缀组成
                             */
                            String hdate = row.get(2).toString().replace("-", "");
                            String serviceCode = row.get(1).toString();
                            String suffix = RandomStringUtils.randomAlphanumeric(8);

                            String rowkey = hdate + "_" + serviceCode + "_" + suffix;
                            return HBaseUtil.getHFileCellListByRow(row, FieldConstant.HBASE_FIELD_MAP.get(TableConstant.HBASE_TABLE_H_COMMUNITY_ANALYSIS), rowkey);
                        }
                    }
            ).sortByKey();

            //HDFS上HFile文件的临时存储目录
            String communityAnalysisHFileStoreHDFSTempPath = Constants.HFILE_TEMP_STORE_PATH + TableConstant.HBASE_TABLE_H_COMMUNITY_ANALYSIS + "/hfile";
            //生成HFile文件并写入HBase
            HBaseUtil.writeData2HBase(hfielRDD, TableConstant.HBASE_TABLE_H_COMMUNITY_ANALYSIS, TableConstant.HBASE_CF_FIELD, communityAnalysisHFileStoreHDFSTempPath);

            System.out.println("---------------" + DateUtils.DATE_FORMAT.format(yesterday) + " ：分析结束---------------");
        }
    }
}
