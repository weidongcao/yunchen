package com.rainsoft.util.java;

import com.rainsoft.manager.ConfManager;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by CaoWeidong on 2017-04-25.
 */
public class JDBCUtils {

    public static Properties getJDBCProp() {
        //Mysql用户名
        String username = ConfManager.getProperty(PropConstants.JDBC_USER);
        //Mysql密码
        String passwd = ConfManager.getProperty(PropConstants.JDBC_PASSWORD);
        //驱动
        String driver = ConfManager.getProperty(PropConstants.JDBC_DRIVER);

        //要传给SPark的Mysql配置
        Properties prop = new Properties();
        //将用户名添加到配置对象
        prop.setProperty("user", username);
        //将密码添加配置对象
        prop.setProperty("password", passwd);
        //将驱动不回配置对象
        prop.setProperty("driver", driver);

        return prop;
    }

    public static Map<String, String> getJDBCMap() {
        Map<String, String> map = new HashMap<>();
        map.put("url", ConfManager.getProperty(PropConstants.JDBC_URL));
        map.put("driver", ConfManager.getProperty(PropConstants.JDBC_DRIVER));
        map.put("user", ConfManager.getProperty(PropConstants.JDBC_USER));
        map.put("password", ConfManager.getProperty(PropConstants.JDBC_PASSWORD));

        return map;
    }
}
