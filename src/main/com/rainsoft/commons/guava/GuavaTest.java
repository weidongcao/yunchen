package com.rainsoft.commons.guava;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import  com.google.common.collect.*;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2017-05-23.
 */
public class GuavaTest {
    public static void main(String[] args) throws IOException {
        Map<String, Map<Long, List<String>>> map = Maps.newHashMap();
        ImmutableList<String> of = ImmutableList.of("aaa", "bbb", "ccc");
//        ArrayList<String> list3 = ImmutableList.of("ccc", "ddd", "eee");
        File file = new File(GuavaTest.class.getResource("/hbase-site.xml").getFile());
        List<String> lines = null;
        lines = Files.readLines(file, Charsets.UTF_8);
        System.out.println(StringUtils.join(lines, ","));
        String[] subdirs = { "usr", "local", "lib" };
        String dir = Joiner.on("/").join(subdirs);
        System.out.println(dir);

        int[] numbers = { 1, 2, 3, 4, 5 };
        String numbersAsString = Joiner.on(";").join(Ints.asList(numbers));
        System.out.println(numbersAsString);

    }
}
