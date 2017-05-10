package com.rainsoft.util.java;

/**
 * HBase常量
 * Created by CaoWeidong on 2017-05-09.
 */
public class TableConstants {

    //BCP数据文件导入记录表
    public static final String MYSQL_IMPORT_BCP_RECORD = "import_bcp_record";

    //HBase的HTTP内容表表名
    public static final String TABLE_H_REG_CONTENT_HTTP = "H_REG_CONTENT_HTTP";

    //HBase的HTTP内容表列簇
    public static final String CF_H_REG_CONTENT_HTTP = "CONTENT_HTTP";

    //HBase的聊天内容表表名
    public static final String TABLE_H_REG_CONTENT_IM_CHAT = "H_REG_CONTENT_IM_CHAT";

    //HBase的聊天内容表列簇
    public static final String CF_H_REG_CONTENT_IM_CHAT = "CONTENT_IM_CHAT";

    //HBase的FTP内容表表名
    public static final String TABLE_H_REG_CONTENT_FTP = "H_REG_CONTENT_FTP";

    //HBase的FTP内容表列簇
    public static final String CF_H_REG_CONTENT_FTP = "CONTENT_FTP";

    //重点区域人群分析表
    public static String HTABLE_EMPHASIS_ANALYSIS = "h_emphasis_analysis";

    //人群分析表
    public static String HTABLE_PEOPLE_ANALYSIS = "h_persion_type";

    //HBase表列簇
    public static String HBASE_CF = "field";

    //小区分析表
    public static String HTABLE_COMMUNITY_ANALYSIS = "h_community_analysis";

}
