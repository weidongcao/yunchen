package com.rainsoft.test.java;

import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2017-04-20.
 */
public class JSONTest {
    public static void main(String[] args) {
        Map<String, Integer> map = new HashMap<>();

        map.put("aaa", 4);
        map.put("bbb", 40);
        map.put("ccc", 400);

        JSONObject jsonObject = new JSONObject(map);
        System.out.println(jsonObject.toString());
    }
}
