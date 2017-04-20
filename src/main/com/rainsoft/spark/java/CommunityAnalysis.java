package com.rainsoft.spark.java;

import com.rainsoft.dao.ICommunityConfigDao;
import com.rainsoft.dao.factory.DaoFactory;
import com.rainsoft.domain.java.CommunityConfig;
import com.rainsoft.manager.ConfManager;
import com.rainsoft.util.java.Constants;
import com.rainsoft.util.java.HBaseUtil;
import com.rainsoft.util.java.NumberUtils;
import com.rainsoft.util.java.StringUtils;
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
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;

/**
 * 说明(默认配置)：
 * 1、实有人口：指昨日出现过的IMSI总数；
 * <p>
 * 2、关注人群：指实有人口中在关注人群库中存在的IMSI数；
 * <p>
 * 3、高危地区人群：指实有人口中归属地在高危地区库中存在的IMSI数；
 * <p>
 * 4、异常常住人群：指10日内未出现过的常住人群；
 * <p>
 * 5、常住人群：昨日出现过的IMSI中，连续出现过7天（含昨天）及以上 或 之前已被标识为常住人群（以最后一次系统或人工标记为准） 的IMSI数；
 * <p>
 * 6、暂住人群：昨日出现过的IMSI中，连续出现过3－6天（含昨天）、且在之前的3个月内未出现过的IMSI数；
 * <p>
 * 7、闪现人群：昨日出现过的IMSI中，连续出现过1－2天（含昨天）、且在之前的3个月内未出现过的IMSI数；
 * <p>
 * 8、其他人群
 * <p>
 * 此外常住人群、暂住人群、闪现人群天数可配置
 */
public class CommunityAnalysis {
    public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    public static SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    //昨天出现的权重
    public static Float WEIGHT_YESTERDAY = ConfManager.getFloat(Constants.WEIGHT_YESTERDAY);
    //增加的权重
    public static Float WEIGHT_ADD = ConfManager.getFloat(Constants.WEIGHT_ADD);
    //减少的权重
    public static Float WEIGHT_REDUCE = ConfManager.getFloat(Constants.WEIGHT_REDUCE);

    public static void main(String[] args) throws Exception {
        String paramDate = args[0];
        int days = Integer.valueOf(args[1]);
        handle(paramDate, days);
    }

    public static void handle(String startDate, int days) throws IOException, ParseException {
        //创建Spark配置对象
        SparkConf conf = new SparkConf()
                .setAppName("小区人群分析");   //Spark应用名

        //创建SparkContext实例
        JavaSparkContext sc = new JavaSparkContext(conf);

        //创建SQLContext
        SQLContext sqlContext = new SQLContext(sc.sc());

        //HiveContext
        //生产代码
//        HiveContext hiveContext = new HiveContext(sc.sc());

        //获取系统当前日期
        Calendar cale = Calendar.getInstance();

        //测试数据
        Date tmp = dateFormat.parse(startDate);
        cale.setTime(tmp);
        //保存在模板文件中的SQL
        String originSql = "";

        System.out.println("---------------当前分析的日期为："+ timeFormat.format(tmp) +"---------------");
        //获取要执行的Hive SQL
        //生产代码
        originSql = FileUtils.readFileToString(new File("sql/communityAnalysis.sql"));
        //测试代码
//            originSql = FileUtils.readFileToString(new File("D:\\0WorkSpace\\JetBrains\\yunchen\\src\\resource\\sql\\communityAnalysis.sql"));

        for (int i = 0; i < days; i++) {
            //昨天日期
            cale.add(Calendar.DATE, -1 - i);
            Date yesterdaty = cale.getTime();

            System.out.println("---------------当前分析的日期为："+ dateFormat.format(yesterdaty) +"---------------");
            //前天日期
            cale.add(Calendar.DATE, -2 - i);
            Date beforeYesterday = cale.getTime();

            //替换昨天的日期
            String hiveSql = originSql.replace("${yesterday}", dateFormat.format(yesterdaty));
            //替换前天的日期
            hiveSql = hiveSql.replace("${before_yesterday}", dateFormat.format(beforeYesterday));

        /*
         * Spark执行Hive sql并返回JavaRDD
         * 返回数据格式：<昨天数据人群IMSI号, 历史数据人员IMSI号, 昨天数据小区名, 昨天数据小区ID, 历史数据小区ID, 昨天数据设备ID, 历史数据人群权重>
         */
            //生产代码
//            JavaRDD<Row> improveDataRDD = hiveContext.sql(hiveSql).javaRDD();
            //测试代码
        JavaRDD<String> originalRDD = sc.textFile("20170310.txt");

            //测试代码
        JavaRDD<Row> improveDataRDD = originalRDD.map(
                new Function<String, Row>() {
                    @Override
                    public Row call(String s) throws Exception {
                        String[] str = s.split("\t");
                        return RowFactory.create(
                                StringUtils.replaceNull(str[0]),
                                StringUtils.replaceNull(str[1]),
                                StringUtils.replaceNull(str[2]),
                                StringUtils.replaceNull(str[3]),
                                StringUtils.replaceNull(str[4]),
                                StringUtils.replaceNull(str[5]),
                                StringUtils.replaceNull(str[6]),
                                StringUtils.replaceNull(str[7]),
                                "null".equals(str[8]) ? null : Float.valueOf(str[8]));
                    }
                }
        );

            /**
             * 分析小区人群
             */
            JavaRDD<Row> handlePeopleRDD = improveDataRDD.map(
                    new Function<Row, Row>() {
                        @Override
                        public Row call(Row row) throws Exception {
                            //昨天数据人群IMSI号
                            String yesterdayIMSI = row.getString(0);
                            //昨天数据小区名
                            String yesterdayServiceName = row.getString(2);
                            //昨天数据小区ID
                            String yesterdayServiceCode = row.getString(4);
                            //昨天数据设备ID
                            String yesterdayMachineID = row.getString(6);
                            //昨天数据人员权重
                            Float yesterdayWeight = null;
                            //昨天的日期
                            String hdate = dateFormat.format(yesterdaty);

                            //历史数据人员IMSI号
                            String historyIMSI = row.getString(1);
                            //历史数据小区名
                            String historyServiceName = row.getString(3);
                            //历史数据小区ID
                            String historyServiceCode = row.getString(5);
                            //历史数据小区ID
                            String historyMachineID = row.getString(7);
                            //历史数据人员权重
                            Float historyWeight = row.getFloat(8);


                            //获取小区配置信息
                            ICommunityConfigDao communityConfigDao = DaoFactory.getCommunityConfigDao();

                            String code = historyServiceCode == null ? yesterdayServiceCode : historyServiceCode;
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

                                    //将历史数据中采集信息的设备ID赋给昨天的数据
                                    yesterdayMachineID = historyMachineID;

                                    if ((historyWeight > 0)     //权重为0
                                            && (historyWeight < (conf.getStayCalcDays() * WEIGHT_ADD))  //最大暂停天数乘以增加的权重
                                            ) {     //其他人群：前天此人没有在此小区出现过(历史数据的权重在0到最大暂停天数乘以增加的权重)
                                    /*
                                     * 判断时间范围是否在3个月内
                                     */
                                        //向上取整，保留小数点后2位
                                        Float addWeight = NumberUtils.getFormatDouble(historyWeight, 2, "up");
                                        //没有出现的天数
                                        int reduceDay = (int) ((addWeight - yesterdayWeight) / WEIGHT_REDUCE);

                                        //如果出现的天数小于最大统计天数,则减小权重
                                        if (reduceDay < ConfManager.getInteger(Constants.COUNT_DAYS)) {
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

                                        boolean flat = yesterdayWeight > (WEIGHT_YESTERDAY + conf.getLongCalcDays() * WEIGHT_ADD);

                                        while (flat) {
                                            yesterdayWeight -= WEIGHT_ADD;
                                            flat = yesterdayWeight > (conf.getLongCalcDays() * WEIGHT_ADD);
                                        }

                                    }

                                } else {    //昨天新增的数据有此人在此小区的信息
                                    if ((historyWeight > 0)     //权重为0
                                            && (historyWeight < (conf.getStayCalcDays() * WEIGHT_ADD))  //最大暂住天数乘以增加的权重
                                            ) {//其他人群：前天此人没有在此小区出现过(历史数据的权重在0到最大暂停天数乘以增加的权重)
                                        //什么都不做

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

                        /*
                         * 返回分析后的小区人员信息
                         * 格式为org.apache.spark.sql.Row
                         * yesterdayIMSI IMSI号 index(0)
                         * yesterdayServiceName 小区名 index(1)
                         * yesterdayServiceCode 小区Code index(2)
                         * yesterdayMachineID 采集设备ID index(3)
                         * hdate 采集日期 index(4)
                         * yesterdayWeight 权重 index(5)
                         */
                            return RowFactory.create(yesterdayIMSI, yesterdayServiceName, yesterdayServiceCode, yesterdayMachineID, hdate, yesterdayWeight);
                        }
                    }
            );

        /*
         * 过滤掉超过最大统计天数(默认为90天)没有出现的小区人员
         */
            JavaRDD<Row> filterHandlePeopleRDD = handlePeopleRDD.filter(
                    (Function<Row, Boolean>) row -> {
                        Float weight = row.getFloat(5);
                        if (0 == weight) {
                            return false;
                        } else {
                            return true;
                        }
                    }
            );

            //缓存数据
            filterHandlePeopleRDD.cache();

        /*
         * 将小区人员分析结果数据写入HBase
         */
            //将分析结果转换成HBase格式的数据
            JavaPairRDD<ImmutableBytesWritable, Put> hbasePeopleAnalysisRDD = filterHandlePeopleRDD.mapToPair(
                    new PairFunction<Row, ImmutableBytesWritable, Put>() {
                        @Override
                        public Tuple2<ImmutableBytesWritable, Put> call(Row row) throws Exception {
                            //HBase数据的rowkey以UUID的格式生成
                            String uuid = UUID.randomUUID().toString().replace("-", "");
                            Put put = new Put(Bytes.toBytes(uuid));
                            String TEMP_CF_PEOPLE_ANALYSIS = ConfManager.getProperty(Constants.CF_PEOPLE_ANALYSIS);
                            //小区人员IMSI号
                            put.addColumn(Bytes.toBytes(TEMP_CF_PEOPLE_ANALYSIS), Bytes.toBytes("imsi"), Bytes.toBytes(row.getString(0)));
                            //小区名
                            put.addColumn(Bytes.toBytes(TEMP_CF_PEOPLE_ANALYSIS), Bytes.toBytes("service_name"), Bytes.toBytes(row.getString(1)));
                            //小区ID
                            put.addColumn(Bytes.toBytes(TEMP_CF_PEOPLE_ANALYSIS), Bytes.toBytes("service_code"), Bytes.toBytes(row.getString(2)));
                            //信息采集设备ID
                            put.addColumn(Bytes.toBytes(TEMP_CF_PEOPLE_ANALYSIS), Bytes.toBytes("machine_id"), Bytes.toBytes(row.getString(3)));
                            //采集日期
                            put.addColumn(Bytes.toBytes(TEMP_CF_PEOPLE_ANALYSIS), Bytes.toBytes("hdate"), Bytes.toBytes(row.getString(4)));
                            //小区人员权重
                            put.addColumn(Bytes.toBytes(TEMP_CF_PEOPLE_ANALYSIS), Bytes.toBytes("weight"), Bytes.toBytes(row.getFloat(5) + ""));

                            //返回HBase格式的数据
                            return new Tuple2<>(new ImmutableBytesWritable(), put);
                        }
                    }
            );

            //获取Hbase的任务配置对象
            JobConf jobConf = HBaseUtil.getHbaseJobConf();
            //设置要插入的HBase表
            jobConf.set(TableOutputFormat.OUTPUT_TABLE, ConfManager.getProperty(Constants.HTABLE_PEOPLE_ANALYSIS));

            //将数据写入HBase
            hbasePeopleAnalysisRDD.saveAsHadoopDataset(jobConf);

        /*
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
                            String hdate = dateFormat.format(yesterdaty);

                            //对同一小区下的数据进行遍历，统计出各类人群
                            for (Row row : tuple._2()) {
                                //小区人员权重
                                Float weight = row.getFloat(5);
                                //小区名
                                serviceName = row.getString(1);
                                //获取小区配置信息DAO
                                ICommunityConfigDao communityConfigDao = DaoFactory.getCommunityConfigDao();

                                //获取小区配置信息实体
                                CommunityConfig conf = communityConfigDao.getConfigByServiceCode(serviceCode);

                                //统计出小区下各类人群数量
                                if ((weight > 0)    //权重大于0
                                        && (weight < (conf.getStayCalcDays() * WEIGHT_ADD))     //权重小于最大暂住天数乘以增加的权重值，默认配置下为6 * 0.1
                                        ) {     //其他人群，默认配置下权重为（0-0.6）

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

                        /*
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
                         */
                            return RowFactory.create(serviceName, serviceCode, hdate, regularCount, temporaryCount, seldomCount, otherCount);
                        }
                    }
            );

            File communityFile = new File("community.txt");
            if (communityFile.exists()) {
                communityFile.delete();
            }
            communityFile.createNewFile();

            communityAnalysisRDD.foreach(
                    new VoidFunction<Row>() {
                        @Override
                        public void call(Row row) throws Exception {
                            String line = row.getString(0) + "\t" + row.getString(1) + "\t" + row.getString(2) + "\t" + row.getInt(3) + "\t" + row.getInt(4) + "\t" + row.getInt(5) + "\t" + row.getInt(6);
                            FileUtils.writeStringToFile(communityFile, line + "\n", true);
                        }
                    }
            );
            communityAnalysisRDD.collect();
            //将小区分析结果转换成Hbase格式的数据
            JavaPairRDD<ImmutableBytesWritable, Put> hbaseCommunityAnalysisRDD = communityAnalysisRDD.mapToPair(
                    new PairFunction<Row, ImmutableBytesWritable, Put>() {
                        @Override
                        public Tuple2<ImmutableBytesWritable, Put> call(Row row) throws Exception {
                            //Hbase的rowkey以UUID的形式自动生成
                            String uuid = UUID.randomUUID().toString().replace("-", "");
                            //创建Hbase数据
                            Put put = new Put(Bytes.toBytes(uuid));

                            String TEMP_CF_COMMUNITY_ANALYSIS = ConfManager.getProperty(Constants.CF_COMMUNITY_ANALYSIS);
                            //添加小区名
                            put.addColumn(Bytes.toBytes(TEMP_CF_COMMUNITY_ANALYSIS), Bytes.toBytes("service_name"), Bytes.toBytes(row.getString(0)));
                            //添加小区ID
                            put.addColumn(Bytes.toBytes(TEMP_CF_COMMUNITY_ANALYSIS), Bytes.toBytes("service_code"), Bytes.toBytes(row.getString(1)));
                            //添加采集日期
                            put.addColumn(Bytes.toBytes(TEMP_CF_COMMUNITY_ANALYSIS), Bytes.toBytes("hdate"), Bytes.toBytes(row.getString(2)));
                            //常住人群数量
                            put.addColumn(Bytes.toBytes(TEMP_CF_COMMUNITY_ANALYSIS), Bytes.toBytes("regular_count"), Bytes.toBytes(row.getInt(3) + ""));
                            //暂停人群数量
                            put.addColumn(Bytes.toBytes(TEMP_CF_COMMUNITY_ANALYSIS), Bytes.toBytes("temporary_count"), Bytes.toBytes(row.getInt(4) + ""));
                            //闪现人群数量
                            put.addColumn(Bytes.toBytes(TEMP_CF_COMMUNITY_ANALYSIS), Bytes.toBytes("seldom_count"), Bytes.toBytes(row.getInt(5) + ""));
                            //其他人群数量
                            put.addColumn(Bytes.toBytes(TEMP_CF_COMMUNITY_ANALYSIS), Bytes.toBytes("other_count"), Bytes.toBytes(row.getInt(6) + ""));

                            return new Tuple2<>(new ImmutableBytesWritable(), put);
                        }
                    }
            );

            //在HBase任务配置对象下设置要写入的表名
            jobConf.set(TableOutputFormat.OUTPUT_TABLE, ConfManager.getProperty(Constants.HTABLE_COMMUNITY_ANALYSIS));

            //写入HBase
            hbaseCommunityAnalysisRDD.saveAsHadoopDataset(jobConf);
            System.out.println("---------------"+ dateFormat.format(yesterdaty) +" ：分析结束---------------");

        }

    }


}
