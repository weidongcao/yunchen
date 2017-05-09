package com.rainsoft.domain.java;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by Administrator on 2017-05-09.
 */
public class ImportBcpRecord implements Serializable {

    private Integer bid;

    private String bcpType;

    private Date importTime;

    private short importStatus;

    public ImportBcpRecord() {
    }

    public ImportBcpRecord(Integer bid, String bcpType, Date importTime, short importStatus) {
        this.bid = bid;
        this.bcpType = bcpType;
        this.importTime = importTime;
        this.importStatus = importStatus;
    }

    public Integer getBid() {
        return bid;
    }

    public void setBid(Integer bid) {
        this.bid = bid;
    }

    public String getBcpType() {
        return bcpType;
    }

    public void setBcpType(String bcpType) {
        this.bcpType = bcpType;
    }

    public Date getImportTime() {
        return importTime;
    }

    public void setImportTime(Date importTime) {
        this.importTime = importTime;
    }

    public short getImportStatus() {
        return importStatus;
    }

    public void setImportStatus(short importStatus) {
        this.importStatus = importStatus;
    }
}
