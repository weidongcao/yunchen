package com.rainsoft.util.java;

import net.sf.json.JSONArray;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2017-05-09.
 */
public class Constants {

    public static final Map<String, JSONArray> HBASE_FIELD_MAP = new HashMap<>();
    //FTP内容表的字段
    public static final String[] FIELD_H_REG_CONTENT_FTP = new String[]{"SERVICEID", "SESSIONID", "CERTIFICATE_TYPE", "CERTIFICATE_CODE", "USERNAME", "SERVICETYPE", "UNAME", "PASSWORD", "FILENAME", "FILE", "OPTYPE", "REMOTEIP", "IS_COMPLETE", "REMOTEPORT", "COMPUTERIP", "COMPUTERPORT", "COMPUTERMAC", "CAPTIME", "ROOM_ID", "CHECKIN_ID", "MACHINE_ID", "DATA_SOURCE", "FILESIZE", "FILEURL"};

    //HTTP内容表的字段
    public static final String[] FIELD_H_REG_CONTENT_HTTP = new String[]{"SERVICEID", "SESSIONID", "CERTIFICATE_TYPE", "CERTIFICATE_CODE", "USERNAME", "SERVICETYPE", "URL", "HOST", "REFURL", "REFHOST", "ACTIONTYPE", "SUBJECT", "SUMMARY", "COOKIE", "ZIPPATH", "UPFILE", "DOWNFILE", "REMOTEIP", "REMOTEPORT", "COMPUTERIP", "COMPUTERPORT", "COMPUTERMAC", "CAPTIME", "ROOM_ID", "CHECKIN_ID", "MACHINE_ID", "DATA_SOURCE"};

    //聊天内容表的字段
    public static final String[] FIELD_H_REG_CONTENT_IM_CHAT = new String[]{"SERVICEID", "SESSIONID", "CERTIFICATE_TYPE", "CERTIFICATE_CODE", "USERNAME", "SERVICETYPE", "UNAME", "ALIAS", "TONAME", "TOALIAS", "FROMNAME", "FROMALIAS", "ACTION", "MESSAGE", "CHATTIME", "REMOTEIP", "REMOTEPORT", "COMPUTERIP", "COMPUTERPORT", "COMPUTERMAC", "CAPTIME", "ROOM_ID", "CHECKIN_ID", "MACHINE_ID", "DATA_SOURCE"};

    //BBS内容表的字段
    public static final String[] FIELD_H_REG_CONTENT_BBS = new String[]{"SERVICEID", "SESSIONID", "CERTIFICATE_TYPE", "CERTIFICATE_CODE", "USERNAME", "SERVICETYPE", "UNAME", "PASSWD", "URL", "HOST", "REFURL", "REFHOST", "POSTING_ID", "TITLE", "AUTHOR", "SOURCE", "ACTIONTYPE", "SUMMARY", "FILE", "REMOTEIP", "REMOTEPORT", "COMPUTERIP", "COMPUTERPORT", "COMPUTERMAC", "CAPTIME", "ROOM_ID", "CHECKIN_ID", "MACHINE_ID", "DATA_SOURCE"};

    //邮件内容表的字段
    public static final String[] FIELD_H_REG_CONTENT_EMAIL = new String[]{"SERVICEID", "SESSIONID", "CERTIFICATE_TYPE", "CERTIFICATE_CODE", "USERNAME", "SERVICETYPE", "UNAME", "PASSWD", "MAILID", "SENDTIME", "FROM", "TO", "CC", "BCC", "SUBJECT", "SUMMARY", "ATTACHMENT", "FILE", "ACTIONTYPE", "REMOTEIP", "REMOTEPORT", "COMPUTERIP", "COMPUTERPORT", "COMPUTERMAC", "CAPTIME", "ROOM_ID", "CHECKIN_ID", "MACHINE_ID", "DATA_SOURCE"};

    //搜索内容表的字段
    public static final String[] FIELD_H_REG_CONTENT_SEARCH = new String[]{"SERVICEID", "SESSIONID", "CERTIFICATE_TYPE", "CERTIFICATE_CODE", "USERNAME", "SERVICETYPE", "URL", "HOST", "REFURL", "REFHOST", "KEYWORD", "KEYWORD_CODE", "REMOTEIP", "REMOTEPORT", "COMPUTERIP", "COMPUTERPORT", "COMPUTERMAC", "CAPTIME", "ROOM_ID", "CHECKIN_ID", "MACHINE_ID", "DATA_SOURCE"};

    //服务内容表的字段
    public static final String[] FIELD_H_SERVICE_INFO = new String[]{"SERVICEID", "SERVICE_NAME", "ADDRESS", "POSTAL_CODE", "PRINCIPAL", "PRINCIPAL_TEL", "INFOR_MAN", "INFOR_MAN_TEL", "INFOR_MAN_EMAIL", "ISP", "STATUS", "ENDING_COUNT", "SERVER_COUNT", "SERVICE_IP", "NET_TYPE", "PRACTITIONER_COUNT", "NET_MONITOR_DEPARTMENT", "NET_MONITOR_MAN", "NET_MONITOR_MAN_TEL", "REMARK", "PROBE_VERSION", "SERVICESTATUS", "COMPUTERNUMS", "SERVICE_TYPE", "MACHINE_ID", "DATA_SOURCE", "LONGITUDE", "LATITUDE", "NETSITE_TYPE", "BUSINESS_NATURE", "LAW_PRINCIPAL_CERTIFICATE_TYPE", "LAW_PRINCIPAL_CERTIFICATE_ID", "START_TIME", "END_TIME", "CODE_ALLOCATION_ORGANIZATION", "POLICE_STATION_CODE", "TEMPLET_VERSION", "SPACE_SIZE", "IP_ADDRESS", "IS_OUTLINE_ALERT", "ELEVATION", "SERVICE_IMG", "AGEN_LAVE_TIME", "REAL_ENDING_NUMS", "CAUSE"};

    //微博内容表的字段
    public static final String[] FIELD_H_REG_CONTENT_WEIBO = new String[]{"SERVICEID", "SESSIONID", "CERTIFICATE_TYPE", "CERTIFICATE_CODE", "USERNAME", "SERVICETYPE", "URL", "HOST", "REFURL", "REFHOST", "KEYWORD", "KEYWORD_CODE", "REMOTEIP", "REMOTEPORT", "COMPUTERIP", "COMPUTERPORT", "COMPUTERMAC", "CAPTIME", "ROOM_ID", "CHECKIN_ID", "MACHINE_ID", "DATA_SOURCE"};
    static {
        JSONArray ftpTableFields = JSONArray.fromObject(Constants.FIELD_H_REG_CONTENT_FTP);
        JSONArray httpTableFields = JSONArray.fromObject(Constants.FIELD_H_REG_CONTENT_HTTP);
        JSONArray imchatTableFields = JSONArray.fromObject(Constants.FIELD_H_REG_CONTENT_IM_CHAT);
        HBASE_FIELD_MAP.put("ftp", ftpTableFields);
        HBASE_FIELD_MAP.put("http", httpTableFields);
        HBASE_FIELD_MAP.put("imchat", imchatTableFields);
    }

    /**
     * 业务相关常量
     */

    //统计的天数
    public static final int COUNT_DAYS = 90;

    //昨天出现权重
    public static final Float WEIGHT_YESTERDAY = 100f;

    //再出现要增加的权重
    public static final Float WEIGHT_ADD = 1f;

    //不出现要减的权重
    public static final Float WEIGHT_REDUCE = 0.01f;

    /**
     * 小区人群表
     */
    //小区实有人群
    public static final String TOTAL_PEOPLE = "total_people";

    //小区常住人群
    public static final String REGULAR_PEOPLE = "regular_people";

    //小区暂停人群
    public static final String TEMPORARY_PEOPLE = "temporary_people";

    //小区闪现人群
    public static final String SELDOM_PEOPLE = "seldom_people";

    /**
     * 数据库连接
     */
    //JDBC驱动
    public static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    //JDBC连接池大小
    public static final int MYSQL_DATASOURCE_SIZE = 6;
    //JDBC连接URL
    public static final String MYSQL_URL = "jdbc:mysql://nn1.hadoop.com:3306/rsdb";
    //JDBC连接用户名
    public static final String MYSQL_USER = "root";
    //JDBC连接密码
    public static final String MYSQL_PASSWORD = "rainsoft";

    /**
     * Solr相关常量
     */
    public static final String MYSQL_COLLECTION = "hbase";

    /**
     * 重点区域相关常量
     */
    //重点区域默认周期
    public static final int EMPHASIS_DOUBTFUL_PERIOD = 5;

    //重点区域最大周期
    public static final int EMPHASIS_MAX_PERIOD = 10;

    //重点区域默认天数
    public static final int EMPHASIS_DOUBTFUL_DAYS = 3;

    //重点区域默认次数
    public static final int EMPHASIS_DOUBTFUL_TIMES = 7;
}
