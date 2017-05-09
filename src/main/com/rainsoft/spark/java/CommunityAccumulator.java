package com.rainsoft.spark.java;

import com.rainsoft.util.java.PropConstants;
import com.rainsoft.util.java.StringUtils;
import org.apache.spark.AccumulatorParam;

/**
 * 小区人群统计
 * Created by caoweidong on 2017/2/15.
 */
public class CommunityAccumulator implements AccumulatorParam<String> {

    /**
     * addInPlace和addAccumulator可以理解为是一样的
     * @param v1 初始化的连接串
     * @param v2 人群类型
     * @return
     */
    @Override
    public String addAccumulator(String v1, String v2) {
        return add(v1, v2);
    }

    @Override
    public String addInPlace(String v1, String v2) {
        return add(v1, v2);
    }

    /**
     * 用于数据的初始化
     * 返回一个值，初始化所有统计值的数据都是0
     * 各个范围人群的统计数量的拼接还是采用key=value|key=value的连接串的格式
     * @param initialValue 不同人群统计数量初始值
     * @return
     */
    @Override
    public String zero(String initialValue) {
        return PropConstants.TOTAL_PEOPLE + "=0|"
                + PropConstants.REGULAR_PEOPLE + "=0|"
                + PropConstants.TEMPORARY_PEOPLE + "=0|"
                + PropConstants.SELDOM_PEOPLE + "=0|";
    }

    /**
     * 小区统计计算逻辑
     * @param v1    连接串
     * @param v2    小区类型
     * @return      更新以后的连接串
     */
    private String add(String v1, String v2) {
        //校验：v1为空的话，直接返回v2
        if (StringUtils.isEmpty(v1)) {
            return v2;
        }

        //使用StringUtils工具类从v1中提取v2对应的值，并累加1
        String oldValue = StringUtils.getValueFromStringByName(v1, "\\|", v2);
        if (null != oldValue) {

            //小区类型统计人数加1
            int newValue = Integer.valueOf(oldValue) + 1;

            //使用StringUtils工具类将v1中v2对应的值设置成新的累加后的值
            return StringUtils.setValueToStringByName(v1, "=","\\|", v2, String.valueOf(newValue));
        }

        return v1;
    }
}