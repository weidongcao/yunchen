package com.rainsoft.util.java;

import java.math.BigDecimal;
import java.text.NumberFormat;

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

    public static String getFormatInt(int maxlength, int minlength, int num) {
        //二进制字符串格式比如3要转换为四位二进制为11，前面要补0
        NumberFormat numFormat = NumberFormat.getInstance();
        //设置是否使用分组
        numFormat.setGroupingUsed(false);
        //设置最大整数位数
        numFormat.setMaximumIntegerDigits(maxlength);
        //设置最小整数位数
        numFormat.setMinimumIntegerDigits(minlength);

        return numFormat.format(num);
    }

    public static void main(String[] args) {
        System.out.println(getFormatInt(2, 2, 7));
    }
}
