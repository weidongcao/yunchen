package com.rainsoft.util.java;

import net.sf.json.JSONArray;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2017-05-10.
 */
public class FieldConstant {

    public static final Map<String, String[]> HBASE_FIELD_MAP = new HashMap<>();

    static {
        //BBS内容表的字段
        HBASE_FIELD_MAP.put("bbs",
                new String[]{"SERVICEID",
                        "SESSIONID",
                        "CERTIFICATE_TYPE",
                        "CERTIFICATE_CODE",
                        "USERNAME",
                        "SERVICETYPE",
                        "UNAME",
                        "PASSWD",
                        "URL",
                        "HOST",
                        "REFURL",
                        "REFHOST",
                        "POSTING_ID",
                        "TITLE",
                        "AUTHOR",
                        "SOURCE",
                        "ACTIONTYPE",
                        "SUMMARY",
                        "FILE",
                        "REMOTEIP",
                        "REMOTEPORT",
                        "COMPUTERIP",
                        "COMPUTERPORT",
                        "COMPUTERMAC",
                        "CAPTIME"}
        );
        //邮件内容表的字段
        HBASE_FIELD_MAP.put("email",
                new String[]{"SERVICEID",
                        "SESSIONID",
                        "CERTIFICATE_TYPE",
                        "CERTIFICATE_CODE",
                        "USERNAME",
                        "SERVICETYPE",
                        "UNAME",
                        "PASSWD",
                        "MAILID",
                        "SENDTIME",
                        "FROM",
                        "TO",
                        "CC",
                        "BCC",
                        "SUBJECT",
                        "SUMMARY",
                        "ATTACHMENT",
                        "FILE",
                        "ACTIONTYPE",
                        "REMOTEIP",
                        "REMOTEPORT",
                        "COMPUTERIP",
                        "COMPUTERPORT",
                        "COMPUTERMAC",
                        "CAPTIME",
                        "ROOM_ID",
                        "CHECKIN_ID",
                        "MACHINE_ID",
                        "DATA_SOURCE"});
        //FTP内容表的字段
        HBASE_FIELD_MAP.put("ftp",
                new String[]{"SERVICEID",
                        "SESSIONID",
                        "CERTIFICATE_TYPE",
                        "CERTIFICATE_CODE",
                        "USERNAME",
                        "SERVICETYPE",
                        "UNAME",
                        "PASSWORD",
                        "FILENAME",
                        "FILE",
                        "OPTYPE",
                        "REMOTEIP",
                        "IS_COMPLETE",
                        "REMOTEPORT",
                        "COMPUTERIP",
                        "COMPUTERPORT",
                        "COMPUTERMAC",
                        "CAPTIME",
                        "ROOM_ID",
                        "CHECKIN_ID",
                        "MACHINE_ID",
                        "DATA_SOURCE",
                        "FILESIZE",
                        "FILEURL"
                });

        //HTTP内容表的字段
        HBASE_FIELD_MAP.put("http",
                new String[]{"SERVICEID",
                        "SESSIONID",
                        "CERTIFICATE_TYPE",
                        "CERTIFICATE_CODE",
                        "USERNAME",
                        "SERVICETYPE",
                        "URL",
                        "HOST",
                        "REFURL",
                        "REFHOST",
                        "ACTIONTYPE",
                        "SUBJECT",
                        "SUMMARY",
                        "COOKIE",
                        "ZIPPATH",
                        "UPFILE",
                        "DOWNFILE",
                        "REMOTEIP",
                        "REMOTEPORT",
                        "COMPUTERIP",
                        "COMPUTERPORT",
                        "COMPUTERMAC",
                        "CAPTIME",
                        "ROOM_ID",
                        "CHECKIN_ID",
                        "MACHINE_ID",
                        "DATA_SOURCE"});

        //聊天内容表的字段
        HBASE_FIELD_MAP.put("im_chat",
                new String[]{"SERVICEID",
                        "SESSIONID",
                        "CERTIFICATE_TYPE",
                        "CERTIFICATE_CODE",
                        "USERNAME",
                        "SERVICETYPE",
                        "UNAME",
                        "ALIAS",
                        "TONAME",
                        "TOALIAS",
                        "FROMNAME",
                        "FROMALIAS",
                        "ACTION",
                        "MESSAGE",
                        "CHATTIME",
                        "REMOTEIP",
                        "REMOTEPORT",
                        "COMPUTERIP",
                        "COMPUTERPORT",
                        "COMPUTERMAC",
                        "CAPTIME",
                        "ROOM_ID",
                        "CHECKIN_ID",
                        "MACHINE_ID",
                        "DATA_SOURCE"});


        //搜索内容表的字段
        HBASE_FIELD_MAP.put("search",
                new String[]{"SERVICEID",
                        "SESSIONID",
                        "CERTIFICATE_TYPE",
                        "CERTIFICATE_CODE",
                        "USERNAME",
                        "SERVICETYPE",
                        "URL",
                        "HOST",
                        "REFURL",
                        "REFHOST",
                        "KEYWORD",
                        "KEYWORD_CODE",
                        "REMOTEIP",
                        "REMOTEPORT",
                        "COMPUTERIP",
                        "COMPUTERPORT",
                        "COMPUTERMAC",
                        "CAPTIME",
                        "ROOM_ID",
                        "CHECKIN_ID",
                        "MACHINE_ID",
                        "DATA_SOURCE"});

        //服务内容表的字段
        HBASE_FIELD_MAP.put("service",
                new String[]{"SERVICEID",
                        "SERVICE_NAME",
                        "ADDRESS",
                        "POSTAL_CODE",
                        "PRINCIPAL",
                        "PRINCIPAL_TEL",
                        "INFOR_MAN",
                        "INFOR_MAN_TEL",
                        "INFOR_MAN_EMAIL",
                        "ISP",
                        "STATUS",
                        "ENDING_COUNT",
                        "SERVER_COUNT",
                        "SERVICE_IP",
                        "NET_TYPE",
                        "PRACTITIONER_COUNT",
                        "NET_MONITOR_DEPARTMENT",
                        "NET_MONITOR_MAN",
                        "NET_MONITOR_MAN_TEL",
                        "REMARK",
                        "PROBE_VERSION",
                        "TEMPLET_VERSION1",
                        "SERVICESTATUS",
                        "COMPUTERNUMS",
                        "SERVICE_TYPE",
                        "MACHINE_ID",
                        "DATA_SOURCE",
                        "LONGITUDE",
                        "LATITUDE",
                        "NETSITE_TYPE",
                        "BUSINESS_NATURE",
                        "LAW_PRINCIPAL_CERTIFICATE_TYPE",
                        "LAW_PRINCIPAL_CERTIFICATE_ID",
                        "START_TIME",
                        "END_TIME",
                        "CODE_ALLOCATION_ORGANIZATION",
                        "POLICE_STATION_CODE",
                        "TEMPLET_VERSION",
                        "SPACE_SIZE",
                        "IP_ADDRESS",
                        "IS_OUTLINE_ALERT",
                        "ELEVATION",
                        "SERVICE_IMG",
                        "AGEN_LAVE_TIME",
                        "REAL_ENDING_NUMS",
                        "CAUSE"});

        //微博内容表的字段
        HBASE_FIELD_MAP.put("weibo",
                new String[]{"SERVICE_CODE_OUT",
                        "SESSIONID",
                        "CERTIFICATE_TYPE",
                        "CERTIFICATE_CODE",
                        "USERNAME",
                        "SERVICETYPE",
                        "UNAME",
                        "PASSWD",
                        "URL",
                        "HOST",
                        "REFURL",
                        "REFHOST",
                        "ID",
                        "TITLE",
                        "AUTHOR",
                        "SOURCE",
                        "ACTIONTYPE",
                        "SUMMARY",
                        "FILE",
                        "REMOTEIP",
                        "REMOTEPORT",
                        "COMPUTERIP",
                        "COMPUTERPORT",
                        "COMPUTERMAC",
                        "CAPTIME",
                        "ROOM_ID",
                        "CHECKIN_ID",
                        "MACHINE_ID",
                        "DATA_SOURCE"});

    }
}
