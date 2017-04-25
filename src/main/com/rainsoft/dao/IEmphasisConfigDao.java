package com.rainsoft.dao;

import com.rainsoft.domain.java.EmphasisConfig;

/**
 * Created by Administrator on 2017-04-25.
 */
public interface IEmphasisConfigDao {

    EmphasisConfig getConfigByServiceCode(String code);
}
