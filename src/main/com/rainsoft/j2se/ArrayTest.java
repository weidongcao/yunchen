package com.rainsoft.j2se;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Arrays;

/**
 * Created by Administrator on 2017-04-27.
 */
public class ArrayTest {
    public static void main(String[] args) throws JSONException {
        int[] arr = new int[7];
        arr[1] = 7;
        System.out.println(Arrays.toString(arr));

        String str = "[0, 7, 0, 0, 0, 0, 0]";

        JSONArray jsonArray = new JSONArray(str);
    }
}
