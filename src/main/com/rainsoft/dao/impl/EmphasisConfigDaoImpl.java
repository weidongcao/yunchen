package com.rainsoft.dao.impl;

import com.rainsoft.dao.IEmphasisConfigDao;
import com.rainsoft.dao.jdbc.JDBCHelper;
import com.rainsoft.domain.java.EmphasisConfig;

import java.sql.ResultSet;
import java.sql.Timestamp;

/**
 * 获取重点区域小区配置对象
 * Created by CaoWeidong on 2017-04-25.
 */
public class EmphasisConfigDaoImpl implements IEmphasisConfigDao {

    /**
     * 根据小区ID获取重点区域小区配置对象
     * @param code
     * @return
     */
    @Override
    public EmphasisConfig getConfigByServiceCode(String code) {
        //重点区域区域配置对象
        EmphasisConfig conf = new EmphasisConfig();

        //查询SQL
        String tmp = "select * from ser_rq_emphasis_area_config where service_code = '${service_code}'";

        //替换参数
        String sql = tmp.replace("${service_code}", code);

        //获取数据库连接
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        //执行查询亲返回对象
        jdbcHelper.executeQueryBySql(sql, rs -> {
            //判断查询结果是否为空
            if (rs.getFetchSize() != 0) {
                //配置ID
                int id = rs.getInt(0);

                //区域ID
                String service_code = rs.getString(1);

                //可疑人群计算周期
                int doubtfulPeriod = rs.getInt(2);
                //可疑人群出现天数
                int doubtfulDays = rs.getInt(3);
                //可疑人群出现次数
                int doubtfulTimes = rs.getInt(4);
                //待发送的消息类型，多个用英文逗号隔开
                String msgTypes = rs.getString(5);
                //消息发送方式json：
                String remindType = rs.getString(6);

                //提醒结束时段：hh:00
                String remindBeginTime = rs.getString(7);

                //提醒结束时段：hh:00
                String remindEndTime = rs.getString(8);
                //创建人id
                int createBy = rs.getInt(9);
                //更新时间
                Timestamp updateTime = rs.getTimestamp(10);

                conf.setId(id);
                conf.setServiceCode(service_code);
                conf.setDoubtful_period(doubtfulPeriod);
                conf.setDoubtful_days(doubtfulDays);
                conf.setDoubtful_times(doubtfulTimes);
                conf.setMsg_types(msgTypes);
                conf.setRemind_type(remindType);
                conf.setRemind_begin_time(remindBeginTime);
                conf.setRemind_end_time(remindEndTime);
                conf.setCreate_by(createBy);
                conf.setUpdate_time(updateTime);
            }
        });

        return conf;
    }
}
