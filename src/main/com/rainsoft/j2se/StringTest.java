package com.rainsoft.j2se;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by Administrator on 2017-04-26.
 */
public class StringTest {
    public static void main(String[] args) {
        System.out.println(RandomStringUtils.randomAlphanumeric(10));
        System.out.println(RandomStringUtils.randomAlphabetic(10));
    }

    public static void testString() {
        String type = "local_window";
        System.out.println(type == "local_window");

        StringBuilder sb = new StringBuilder();
        sb.append("aa");
        sb.append(System.lineSeparator());
        sb.append("bb");
        sb.append(System.lineSeparator());
        sb.append(System.lineSeparator());
        sb.append(System.lineSeparator());
        sb.append("cc");
        sb.append(System.lineSeparator());
        System.out.println(sb.toString().replaceAll("(?m)^\\s*$(\\n|\\r\\n)", ""));

        String aaa = "1478334";
        System.out.println(aaa.substring(5, 7));
    }
}
