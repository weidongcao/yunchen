package com.rainsoft.dao.impl;

import com.rainsoft.dao.ICommunityConfigDao;
import com.rainsoft.dao.jdbc.JDBCHelper;
import com.rainsoft.domain.java.CommunityConfig;

import java.sql.ResultSet;
import java.sql.Timestamp;

/**
 * Created by Administrator on 2017-04-17.
 */
public class CommunityConfigDaoImpl implements ICommunityConfigDao {
    @Override
    public CommunityConfig getConfigByServiceCode(String code) {
        CommunityConfig conf = new CommunityConfig();
        conf.setLongCalcDays(7);
        conf.setStayCalcDays(6);
        conf.setNewCalcDays(2);

        String tmp = "select * from ser_rq_community_config where service_code = '${service_code}'";

        String sql = tmp.replace("${service_code}", code);
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeQueryBySql(sql, new JDBCHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                if (rs.getFetchSize() != 0){
                    long id = rs.getLong(0);

                    String service_code = rs.getString(1);

                    int longCalcDays = rs.getInt(2);
                    int stayCalcDays = rs.getInt(3);
                    int newCalcDays = rs.getInt(4);
                    int longDisappearWarnDays = rs.getInt(5);
                    int longDisappearClearDays = rs.getInt(6);

                    String msgTypes = rs.getString(7);
                    String remindType = rs.getString(8);
                    String createBy = rs.getString(9);
                    Timestamp updateTime = rs.getTimestamp(10);

                    conf.setId(id);
                    conf.setService_code(service_code);
                    conf.setLongCalcDays(longCalcDays);
                    conf.setStayCalcDays(stayCalcDays);
                    conf.setNewCalcDays(newCalcDays);
                    conf.setLongDisappearWarnDays(longDisappearWarnDays);
                    conf.setLongDisappearClearDays(longDisappearClearDays);
                    conf.setMsgTypes(msgTypes);
                    conf.setRemindType(remindType);
                    conf.setCreateBy(createBy);
                    conf.setUpdateTime(updateTime);
                }
            }
        });

        return conf;
    }
}
