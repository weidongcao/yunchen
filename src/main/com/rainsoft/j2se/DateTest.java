package com.rainsoft.j2se;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by Administrator on 2017-05-03.
 */
public class DateTest {
    public static void main(String[] args) throws ParseException {
        DateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(timeFormat.parse("2017-05-24 19:30:02"));
        Date curDate = calendar.getTime();
        System.out.println("获取日历的小时（24小时制）" + calendar.get(Calendar.HOUR_OF_DAY));
        System.out.println("通过Date类获取小时" + curDate.getHours());
        System.out.println("通过Date类获取天（一个月内的第几天）" + curDate.getDate());
        System.out.println("通过Date类获取天（一周内的第几天）" + curDate.getDay());
    }
}
