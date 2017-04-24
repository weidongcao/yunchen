package com.rainsoft.util.java;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IMSIUtils {
    public static String getMobileAll(String imsi) {
        String s130 = "^46001(\\d{3})(\\d)[0,1]\\d+";
        String s131 = "^46001(\\d{3})(\\d)9\\d+";
        String s132 = "^46001(\\d{3})(\\d)2\\d+";
        String s134 = "^460020(\\d)(\\d{3})\\d+";
        String s13x0 = "^46000(\\d{3})([5,6,7,8,9])\\d+";
        String s13x = "^46000(\\d{3})([0,1,2,3,4])(\\d)\\d+";
        String s150 = "^460023(\\d)(\\d{3})\\d+";
        String s151 = "^460021(\\d)(\\d{3})\\d+";
        String s152 = "^460022(\\d)(\\d{3})\\d+";
        String s155 = "^46001(\\d{3})(\\d)4\\d+";
        String s156 = "^46001(\\d{3})(\\d)3\\d+";
        String s157 = "^460077(\\d)(\\d{3})\\d+";
        String s158 = "^460028(\\d)(\\d{3})\\d+";
        String s159 = "^460029(\\d)(\\d{3})\\d+";
        String s147 = "^460079(\\d)(\\d{3})\\d+";
        String s185 = "^46001(\\d{3})(\\d)5\\d+";
        String s186 = "^46001(\\d{3})(\\d)6\\d+";
        String s187 = "^460027(\\d)(\\d{3})\\d+";
        String s188 = "^460078(\\d)(\\d{3})\\d+";
        String s1705 = "^460070(\\d)(\\d{3})\\d+";
        String s170x = "^46001(\\d{3})(\\d)8\\d+";
        String s178 = "^460075(\\d)(\\d{3})\\d+";
        String s145 = "^46001(\\d{3})(\\d)7\\d+";
        String s182 = "^460026(\\d)(\\d{3})\\d+";
        String s183 = "^460025(\\d)(\\d{3})\\d+";
        String s184 = "^460024(\\d)(\\d{3})\\d+";
        // 电信的，下面的还没有找到规则
        String s180 = "^46003(\\d)(\\d{3})7\\d+";
        String s153 = "^46003(\\d)(\\d{3})8\\d+";
        String s189 = "^46003(\\d)(\\d{3})9\\d+";

        String[] result = compile(s130, imsi);
        if (result != null && result.length == 2) {
            return "130" + result[1] + result[0];
        }
        result = compile(s131, imsi);
        if (result != null && result.length == 2) {
            return "131" + result[1] + result[0];
        }
        result = compile(s132, imsi);
        if (result != null && result.length == 2) {
            return "132" + result[1] + result[0];
        }
        result = compile(s134, imsi);
        if (result != null && result.length == 2) {
            return "134" + result[0] + result[1];
        }
        result = compile(s13x0, imsi);
        if (result != null && result.length == 2) {
            return "13" + result[1] + "0" + result[0];
        }
        result = compile(s13x, imsi);
        if (result != null && result.length == 3) {
            return "13" + (Integer.parseInt(result[1]) + 5) + result[2] + result[0];
        }
        result = compile(s150, imsi);
        if (result != null && result.length == 2) {
            return "150" + result[0] + result[1];
        }
        result = compile(s151, imsi);
        if (result != null && result.length == 2) {
            return "151" + result[0] + result[1];
        }
        result = compile(s152, imsi);
        if (result != null && result.length == 2) {
            return "152" + result[0] + result[1];
        }
        result = compile(s155, imsi);
        if (result != null && result.length == 2) {
            return "155" + result[1] + result[0];
        }
        result = compile(s156, imsi);
        if (result != null && result.length == 2) {
            return "156" + result[1] + result[0];
        }
        result = compile(s157, imsi);
        if (result != null && result.length == 2) {
            return "157" + result[0] + result[1];
        }
        result = compile(s158, imsi);
        if (result != null && result.length == 2) {
            return "158" + result[0] + result[1];
        }
        result = compile(s159, imsi);
        if (result != null && result.length == 2) {
            return "159" + result[0] + result[1];
        }
        result = compile(s147, imsi);
        if (result != null && result.length == 2) {
            return "147" + result[0] + result[1];
        }
        result = compile(s185, imsi);
        if (result != null && result.length == 2) {
            return "185" + result[1] + result[0];
        }
        result = compile(s186, imsi);
        if (result != null && result.length == 2) {
            return "186" + result[1] + result[0];
        }
        result = compile(s187, imsi);
        if (result != null && result.length == 2) {
            return "187" + result[0] + result[1];
        }
        result = compile(s188, imsi);
        if (result != null && result.length == 2) {
            return "188" + result[0] + result[1];
        }
        result = compile(s1705, imsi);
        if (result != null && result.length == 2) {
            return "170" + result[0] + result[1];
        }
        result = compile(s170x, imsi);
        if (result != null && result.length == 2) {
            return "170" + result[1] + result[0];
        }
        result = compile(s178, imsi);
        if (result != null && result.length == 2) {
            return "178" + result[0] + result[1];
        }
        result = compile(s145, imsi);
        if (result != null && result.length == 2) {
            return "145" + result[1] + result[0];
        }
        result = compile(s182, imsi);
        if (result != null && result.length == 2) {
            return "182" + result[0] + result[1];
        }
        result = compile(s183, imsi);
        if (result != null && result.length == 2) {
            return "183" + result[0] + result[1];
        }
        result = compile(s184, imsi);
        if (result != null && result.length == 2) {
            return "184" + result[0] + result[1];
        }
        result = compile(s180, imsi);
        if (result != null && result.length == 2) {
            return "180" + result[0] + result[1];
        }
        result = compile(s153, imsi);
        if (result != null && result.length == 2) {
            return "153" + result[0] + result[1];
        }
        result = compile(s189, imsi);
        if (result != null && result.length == 2) {
            return "189" + result[0] + result[1];
        }
        return imsi;
    }

    private static String[] compile(String reg, String imsi) {
        Pattern pattern = Pattern.compile(reg);
        Matcher matcher = pattern.matcher(imsi);

        if (matcher.find()) {
            String[] sArr = new String[matcher.groupCount()];
            for (int i = 0; i < matcher.groupCount(); i++) {
                sArr[i] = matcher.group(i + 1);
            }
            return sArr;
        }
        return null;
    }

    /**
     * 通过IMSI号生成
     *
     * @param sc
     * @param sqlContext
     * @param imsiRDD
     * @return
     */
    public static DataFrame createIMSIKeyArea(JavaSparkContext sc, SQLContext sqlContext, JavaRDD<String> imsiRDD, String type) {

        if (type.startsWith("local")) {
            JavaRDD<String> phoneAreaRDD = null;
            //读取手机号前7位相关信息
            if (type.startsWith("local_window")) {
                phoneAreaRDD = sc.textFile("file:///D:\\0WorkSpace\\Develop\\data\\phone_to_area_30w");
            } else if (type.equals("local_linux")) {
                phoneAreaRDD = sc.textFile("file:///phone_to_area_30w");
            }


            /*
             * 将手机号前7位及其相关信息注册为临时表
             */
            //手机号前7位表数据
            JavaRDD<Row> phoneAreaRowRDD = phoneAreaRDD.map(
                    (Function<String, Row>) s -> {
                        String[] arr = s.split("\t");
                        return RowFactory.create(arr[0], arr[1], arr[2], arr[3], arr[4]);
                    }
            );

            //手机号前7位表元数据
            StructType phoneSchema = DataTypes.createStructType(Arrays.asList(
                    //手机号前7位字段
                    DataTypes.createStructField("phone_num", DataTypes.StringType, true),
                    //地区名字段
                    DataTypes.createStructField("area_name", DataTypes.StringType, true),
                    //地区代码字段
                    DataTypes.createStructField("area_code", DataTypes.StringType, true),
                    //运营商字段
                    DataTypes.createStructField("phone_type", DataTypes.StringType, true),
                    //电话区号字段
                    DataTypes.createStructField("region", DataTypes.StringType, true)
            ));

            //创建DataFrame对象
            DataFrame phoneDF = sqlContext.createDataFrame(phoneAreaRowRDD, phoneSchema);

            //注册为临时表
            phoneDF.registerTempTable("yuncai.h_sys_phone_to_area");

        }

        /*
         * 通过IMSI号生成手机号前7位，并注册为临时表
         */
        //生成IMSI临时表数据
        JavaRDD<Row> imsiRowRDD = imsiRDD.map(
                (Function<String, Row>) s -> {
                    String phone7 = IMSIUtils.getMobileAll(s);
                    return RowFactory.create(phone7, s);
                }
        );

        //生成IMSI临时表元数据
        StructType imsiSchema = DataTypes.createStructType(Arrays.asList(
                //手机号前7位的表头
                DataTypes.createStructField("phone_num", DataTypes.StringType, true),
                //IMSI号的表头
                DataTypes.createStructField("imsi", DataTypes.StringType, true)
        ));

        //创建DataFrame
        DataFrame imsiDF = sqlContext.createDataFrame(imsiRowRDD, imsiSchema);

        //注册为临时表
        imsiDF.registerTempTable("imsiTable");


        //两个表join的sql
        String sql = "select i.imsi, p.phone_num, p.area_name, p.area_code, p.phone_type, p.region from imsiTable i left join yuncai.h_sys_phone_to_area p on i.phone_num = p.phone_num";

        //两个临时表进行join
        DataFrame infoDF = sqlContext.sql(sql);

        return infoDF;
    }


}
