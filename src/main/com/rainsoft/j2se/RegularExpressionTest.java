package com.rainsoft.j2se;

/**
 * Created by Administrator on 2017-05-03.
 */
public class RegularExpressionTest {
    public static void main(String[] args) {
        String aaa = "460017815206975,1452556,山东省青岛市,370200,联通";
        String bbb = aaa.replaceAll("[(移动)(联通)(电信)]", "");
        System.out.println(aaa.replaceAll("[(移动)(联通)(电信)]", ""));
    }

}
