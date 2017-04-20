package com.rainsoft.manager;

import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理组件
 *
 * @author caoweidong
 */
public class ConfManager {

    //Properties对象
    private static Properties prop = new Properties();

    static {
        try {
            //通过类名.class的方式，获取到这个类在JVM中对应的Class对象
            //然后通过这个Class对象的getClassLoader()方法，获取到当初加载这个类的JVM中的类加载器（ClassLoader）
            //调用ClassLoader的getResourceAsStream()方法，可以用类加载器去加载类加载路径中指定的文件
            //最终可以获取到一个，针对指定文件的输入流（InputStream）
            InputStream in = ConfManager.class.getClassLoader().getResourceAsStream("config.properties");

            //调用Properties的load()方法，给它传入一个文件的InputStream输入流
            //将文件中的符合“key=value”格式的配置项都加载到Properies对象中
            prop.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取指定key对应的value
     * @param key properties配置文件的key
     * @return properties配置文件的value（String）
     */
    public static String getProperty(String key) {
        return prop.getProperty(key);
    }

    /**
     * 获取整数类型的配置项
     * @param key properties配置文件的key
     * @return properties配置文件的value(Int类型)
     */
    public static Integer getInteger(String key) {
        String value = getProperty(key);
        try {
            return Integer.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }
    /**
     * 获取Float类型的配置项
     * @param key properties配置文件的key
     * @return properties配置文件的value(Float类型)
     */
    public static Float getFloat(String key) {
        String value = getProperty(key);
        try {
            return Float.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0f;
    }
    /**
     * 获取布尔类型的配置项
     * @param key properties配置文件的key
     * @return properties配置文件的value(boolean类型)
     */
    public static Boolean getBoolean(String key) {
        String value = getProperty(key);
        try {
            return Boolean.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public static void main(String[] args) {
        Float f1 = 1f;
        Float f2 = 2f;
        Float f3 = 4f;
        System.out.println(f3 > (f1 + f2));
    }
}