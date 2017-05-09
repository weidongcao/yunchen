package com.rainsoft.dao.impl;

import com.rainsoft.dao.IImportBcpRecordDao;
import com.rainsoft.dao.jdbc.JDBCHelper;
import com.rainsoft.domain.java.ImportBcpRecord;
import com.sun.corba.se.impl.encoding.CDROutputObject;
import com.sun.xml.bind.v2.schemagen.xmlschema.Import;

import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Administrator on 2017-05-09.
 */
public class ImportBcpRecordDaoImpl implements IImportBcpRecordDao {
    public static DateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * 根据BCP数据的类型和日期查询导入记录
     * @param bcpType
     */
    @Override
    public ImportBcpRecord getRecord(String bcpType) {

        ImportBcpRecord record = new ImportBcpRecord();

        String temple = "select * from import_bcp_record where bcp_type = '${bcp_type}' order by import_time desc limit 1";

        String sql = temple.replace("${bcp_type}", bcpType);

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        jdbcHelper.executeQueryBySql(sql, new JDBCHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                if (rs.next()) {
                    int bid = rs.getInt(0);
                    String bcpType = rs.getString(1);
                    Date importTime = timeFormat.parse(rs.getString(2));
                    short importStatus = rs.getShort(3);

                    record.setBid(bid);
                    record.setBcpType(bcpType);
                    record.setImportTime(importTime);
                    record.setImportStatus(importStatus);
                }
            }
        });

        if (0 == record.getBid()) {
            return null;
        } else {
            return record;
        }
    }

    /**
     * 插入一条记录
     * @param record
     */
    @Override
    public void insertRecord(ImportBcpRecord record) {

        String temple = "insert into import_bcp_record values(null, ?, ?, ?)";

        Object[] params = new Object[]{
                record.getBcpType(),
                record.getBcpType(),
                record.getImportTime(),
                record.getImportStatus(),
        };

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdateByParams(temple, params);
    }
}
