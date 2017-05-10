package com.rainsoft.util.java;

import org.apache.hadoop.conf.Configuration;

import java.util.Properties;

/**
 * 常量类
 * Created by Administrator on 2017-04-10.
 */
public class PropConstants {

    //重点区域刷新时长
    public static String EMPHASIS_TIME_INTERVAL = "emphasis_time_interval";

    //将BCP数据文件导入到HBase的时间间隔
    public static String IMPORT_BCP_DATA_TIME_INTERVAL = "import_bcp_data_time_interval";

    //BCP文件所有目录
    public static String DIR_BCP_RESOURCE = "dir_bcp_path";

}
