package com.rainsoft.util.java;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * 日期工具类
 */
public class DateUtils {
    public static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    public static final DateFormat TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    public static final DateFormat HOUR_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH");

    /**
     * 判断字符串是否为日期
     * @param str
     * @param dateFormat
     * @return
     */
    public static boolean isDate(String str, DateFormat dateFormat) {
        try {
            dateFormat.parse(str);
        } catch (ParseException e) {
            return false;
        }
        return true;
    }
}
