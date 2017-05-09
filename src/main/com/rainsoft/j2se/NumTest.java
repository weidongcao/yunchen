package com.rainsoft.j2se;

import java.text.NumberFormat;

/**
 * Created by Administrator on 2017-04-24.
 */
public class NumTest {
    public static void main(String[] args) {
         //待测试数据
        int i = 7;
        //得到一个NumberFormat的实例
        NumberFormat nf = NumberFormat.getInstance();
        //设置是否使用分组
        nf.setGroupingUsed(false);
        //设置最大整数位数
        nf.setMaximumIntegerDigits(4);
        //设置最小整数位数
        nf.setMinimumIntegerDigits(4);
        //输出测试语句
        System.out.println(nf.format(Integer.valueOf(Integer.toBinaryString(Integer.valueOf(7)))));
    }
}
