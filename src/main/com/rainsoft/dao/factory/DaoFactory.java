package com.rainsoft.dao.factory;

import com.rainsoft.dao.ICommunityConfigDao;
import com.rainsoft.dao.IEmphasisConfigDao;
import com.rainsoft.dao.impl.CommunityConfigDaoImpl;
import com.rainsoft.dao.impl.EmphasisConfigDaoImpl;

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
    /**
     * 获取重点区域区域配置Dao
     * @return
     */
    public static IEmphasisConfigDao getemphasisConfigDao() {
        return new EmphasisConfigDaoImpl();
    }

}
