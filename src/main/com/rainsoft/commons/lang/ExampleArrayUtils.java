package com.rainsoft.commons.lang;


import org.apache.commons.lang3.ArrayUtils;

/**
 * Created by Administrator on 2017-05-25.
 */
public class ExampleArrayUtils {
    public static void main(String[] args) {
        // data setup
        int[] intArray1 = {2, 4, 8, 16};
        int[][] intArray2 = {{1, 2}, {2, 4}, {3, 8}, {4, 16}};
        Object[][] notAMap = {
                {"A", new Double(100)},
                {"B", new Double(80)},
                {"C", new Double(60)},
                {"D", new Double(40)},
                {"E", new Double(20)}
        };

        //打印数组
        System.out.println("intArray1: " + ArrayUtils.toString(intArray1));
        System.out.println("intArray2: " + ArrayUtils.toString(intArray2));
        System.out.println("notAMap: " + ArrayUtils.toString(notAMap));

        //查找元素
        int[] aaa = ArrayUtils.add(intArray1, 99);
        System.out.println("array add: " + ArrayUtils.toString(aaa));
    }
}
