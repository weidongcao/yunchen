package com.rainsoft.j2se;

import net.sf.json.JSONArray;

/**
 * Created by Administrator on 2017-05-02.
 */
public class JSONTest {
    public static void main(String[] args) {
        int[] arr = new int[]{1, 4, 8, 0, 0, 1};
        JSONArray jsonArray = JSONArray.fromObject("[]");

        String isarea = "[56,X,54,53,52,51,50,37,49,48,47,46,45,44,43,42,41,40,36,39]";
        JSONArray isareaJSON = JSONArray.fromObject(isarea);

        jsonArray.add("X");
        jsonArray.add(9);
        System.out.println("测试JSONArray占位符optInt ： " + jsonArray.optInt(0));
        System.out.println("测试JSONArray占位符optInt ： " + jsonArray.optInt(1));
        System.out.println("测试JSONArray占位符optString ： " + jsonArray.optString(0));
        System.out.println("测试JSONArray占位符optString ： " + jsonArray.optString(1));
//        System.out.println(serviceJsonArray.optString(0));
        int aaa = 56;
        System.out.println(isareaJSON.contains(aaa));

    }
}
