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
        //测试user表
        HBASE_FIELD_MAP.put("user", new String[]{
                "name",
                "age",
                "city",
                "home"
        });

        //高危地区表字段
        HBASE_FIELD_MAP.put("h_ser_rq_danger_area",
                new String[]{
                        "area",
                        "brief",
                        "create_time",
                        "update_time",
                        "create_by"
                });
        //高危人群表字段
        HBASE_FIELD_MAP.put("h_ser_rq_danger_person",
                new String[]{
                        "type",
                        "rank",
                        "brief",
                        "id_no",
                        "imsi",
                        "phone",
                        "name",
                        "create_time",
                        "update_time",
                        "create_by"
                });
        //小区人群统计表字段
        HBASE_FIELD_MAP.put("h_community_analysis",
                new String[]{
                        "service_name",
                        "service_code",
                        "regular_count",
                        "temporary_count",
                        "seldom_count",
                        "other_count",
                        "danger_persion_count",
                        "danger_area_count",
                        "hdate"
                });
        //小区人群分析表字段
        HBASE_FIELD_MAP.put("h_persion_type",
                new String[]{
                        "service_name",
                        "service_code",
                        "imsi",
                        "weight",
                        "hdate"
                });
        //云嗅设备采集表
        HBASE_FIELD_MAP.put("h_scan_ending_improve",
                new String[]{
                        "EQUIPMENT_MAC",
                        "IMSI_CODE",
                        "CAPTURE_TIME",
                        "OPERATOR_TYPE",
                        "SN_CODE",
                        "LONGITUDE",
                        "LATITUDE",
                        "FIRST_TIME",
                        "LAST_TIME",
                        "DIST",
                        "IMPORT_TIME",
                        "PHONE_AREA"
                });
        //区域碰撞结果表字段
        HBASE_FIELD_MAP.put("h_collision_result",
                new String[]{
                        "imsi_code",
                        "area_name",
                        "phone_type",
                        "danger_people",
                        "appear_times"
                });
        //区域碰撞任务表字段
        HBASE_FIELD_MAP.put("h_collision_job",
                new String[]{
                        "start_time",
                        "end_time",
                        "createperson",
                        "condition",
                        "job_status"
                });
        //重点区域分析结果表字段
        HBASE_FIELD_MAP.put("h_emphasis_analysis_result",
                new String[]{
                        "service_name",
                        "service_code",
                        "stat_date",
                        "total_people",
                        "suspicious_people",
                        "attention_people",
                        "danger_area_people"
                });

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
