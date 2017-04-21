package com.rainsoft.j2se;

import org.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2017-04-14.
 */
public class IntTest {
    public static void main(String[] args) {
        System.out.println("Integer.MAX_VALUE = " + Integer.MAX_VALUE);
    }

    /**
     * Integer类型的最大值
     *21亿4千7百4十8万3千6百4十7
     * @return
     */
    public static Integer getMaxInt() {
        return Integer.MAX_VALUE;
    }

    /**
     * Created by Administrator on 2017-04-20.
     */
    public static class JSONTest {
        public static void main(String[] args) {
            Map<String, Integer> map = new HashMap<>();

            map.put("aaa", 4);
            map.put("bbb", 40);
            map.put("ccc", 400);

            JSONObject jsonObject = new JSONObject(map);
            System.out.println(jsonObject.toString());
        }
    }

    /**
     * Created by Administrator on 2017-04-17.
     */
    public static class NumTest {
        public static void main(String[] args) {
            Float f1 = 0.34f;
            Float f2 = 0.44f;
            Float f3 = 0.55f;
            System.out.println("Math.floor(f1) = " + Math.floor(f1));
            System.out.println("Math.rint(f3) = " + Math.rint(f3));
            System.out.println(f2 + "");
        }
    }

    /**
     * Created by Administrator on 2017-04-11.
     */
    public static class TimeTest {
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
}
