package com.rainsoft.util.java;

import java.math.BigDecimal;

/**
 * 数字格式工具类
 * Created by caoweidong on 2017/2/5.
 */
public class NumberUtils {
    /**
     * 将数字格式化为指定位数的小数
     *
     * @param num
     * @param scale 四舍五入的位数
     * @param type 取整类型（round:四舍五入; down:向下取整; up:向上取整）
     * @return 格式化的小数
     */
    public static Float getFormatDouble(float num, int scale, String type) {
        BigDecimal bd = new BigDecimal(num);
        Float res = num;
        if ("round".equals(type)) {
            res =  bd.setScale(scale, BigDecimal.ROUND_HALF_UP).floatValue();
        } else if ("down".equals(type)) {
            res = bd.setScale(scale, BigDecimal.ROUND_DOWN).floatValue();
        } else if ("up".equals(type)) {
            res = bd.setScale(scale, BigDecimal.ROUND_UP).floatValue();
        }
        return res;
    }

}
