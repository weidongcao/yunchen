package com.rainsoft.dao;

import com.rainsoft.domain.java.CommunityConfig;

/**
 * Created by Administrator on 2017-04-17.
 */
public interface ICommunityConfigDao {

    CommunityConfig getConfigByServiceCode(String code);
}
