package com.rainsoft.dao;

import com.rainsoft.domain.java.ImportBcpRecord;

import java.util.Date;

/**
 * Created by Administrator on 2017-05-09.
 */
public interface IImportBcpRecordDao {

    ImportBcpRecord getRecord(String bcpType);

    void insertRecord(ImportBcpRecord record);
}
