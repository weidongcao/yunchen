package com.rainsoft.j2se;

import java.util.Random;

/**
 * Java二进制与十进制十六进制的转换
 * Created by Administrator on 2017-04-21.
 */
public class JinZhi {
    public static void main(String[] args) {
//        init();
        Float f1 = 3.00f;
        int  i = f1.intValue();
        System.out.println(f1 - i == 0);

    }

    public static void init() {
        Random random = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 90; i++) {
            int tmp = random.nextInt();
            if (tmp % 2 == 0) {
                sb.append(1);
            } else if (tmp % 2 == 1) {
                sb.append(0);
            }
        }
        sb.append("1000");
//        System.out.println("Int最大值 = " + Integer.MAX_VALUE);
        System.out.println("二进制 = " + sb.toString());
        System.out.println("二进制转十进制long = " + Long.valueOf(sb.toString(), 2));
//        System.out.println("二进制转十进制int = " + Integer.valueOf(sb.toString(), 2));
        System.out.println("二进制转十六进制 = " + Integer.toHexString(Integer.parseInt(sb.toString(), 2)));
    }
}
