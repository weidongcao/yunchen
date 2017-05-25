package com.rainsoft.hbase.java;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * Created by Administrator on 2017-05-12.
 */
public class Test {
    public static void main(String[] args) throws IOException {
        File file = new File("D:\\0WorkSpace\\Develop\\data\\hfile\\http\\aa\\bbb.bcp");
        String str = FileUtils.readFileToString(file);
        String aaa = str.replace("|$|", "");

        System.out.println(str);
        System.out.println(aaa);
    }
}
