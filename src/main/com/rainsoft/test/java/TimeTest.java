package com.rainsoft.test.java;

import javafx.scene.input.DataFormat;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by Administrator on 2017-04-11.
 */
public class TimeTest {
    public static void main(String[] args) {
        SimpleDateFormat dataFormat = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
        Calendar cala = Calendar.getInstance();
        Date cur = cala.getTime();
        System.out.println("cala = " + cala);
        System.out.println(dataFormat.format(cur) + " --> " + cur.getTime());
        cala.add(Calendar.DATE, -1);
        Date yes = cala.getTime();
        System.out.println(dataFormat.format(yes) + " --> " + yes.getTime());

    }
}
