package com.rainsoft.dao.factory;

import com.rainsoft.dao.ICommunityConfigDao;
import com.rainsoft.dao.impl.CommunityConfigDaoImpl;

/**
   */
public class DaoFactory {

    /**
     * 获取小区配置Dao
     * @return
     */
    public static ICommunityConfigDao getCommunityConfigDao() {
        return new CommunityConfigDaoImpl();
    }

}
