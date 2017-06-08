package com.rainsoft.util.java;

import org.apache.hadoop.hbase.util.Bytes;

public class TableConstant {
    //HBase表列簇:info
	public static final String HBASE_CF_INFO = "info";

    //HBase表列簇:field
	public static final String HBASE_CF_FIELD = "field";

    //小区人群分析表
	public static final String HBASE_TABLE_H_PERSION_TYPE = "h_persion_type";

    //小区人群分析表结果表
	public static final String HBASE_TABLE_H_COMMUNITY_ANALYSIS = "h_community_analysis";

	//区域碰撞任务表
	public static final String HBASE_TABLE_H_COLLISION_JOB = "h_collision_job";

	//区域碰撞结果表
	public static final String HBASE_TABLE_H_COLLISION_RESULT = "h_collision_result";

    //user表(测试)
    public static final String HBASE_TABLE_USER_NAME = "user";

    //云嗅终端采集数据表
    public static final String HBASE_TABLE_H_SCAN_ENDING_IMPROVE = "H_SCAN_ENDING_IMPROVE";

    //重点区域分析结果表
    public static final String HBASE_TABLE_H_EMPHASIS_ANALYSIS_RESULT = "h_emphasis_analysis_result";

    //重点区域采集数据Hive临时存储表
    public static final String HIVE_TABLE_BUFFER_EMPHASIS_ANALYSIS = "buffer_emphasis_analysis";


}
