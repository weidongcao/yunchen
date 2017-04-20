package com.rainsoft.domain.java;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * Created by Administrator on 2017-04-17.
 */
public class CommunityConfig implements Serializable {

    private long id;

    private String service_code;

    private int longCalcDays;

    private int stayCalcDays;

    private int newCalcDays;

    private int longDisappearWarnDays;

    private int longDisappearClearDays;

    private String msgTypes;

    private String remindType;

    private String createBy;

    private Timestamp updateTime;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getService_code() {
        return service_code;
    }

    public void setService_code(String service_code) {
        this.service_code = service_code;
    }

    public int getLongCalcDays() {
        return longCalcDays;
    }

    public void setLongCalcDays(int longCalcDays) {
        this.longCalcDays = longCalcDays;
    }

    public int getStayCalcDays() {
        return stayCalcDays;
    }

    public void setStayCalcDays(int stayCalcDays) {
        this.stayCalcDays = stayCalcDays;
    }

    public int getNewCalcDays() {
        return newCalcDays;
    }

    public void setNewCalcDays(int newCalcDays) {
        this.newCalcDays = newCalcDays;
    }

    public int getLongDisappearWarnDays() {
        return longDisappearWarnDays;
    }

    public void setLongDisappearWarnDays(int longDisappearWarnDays) {
        this.longDisappearWarnDays = longDisappearWarnDays;
    }

    public int getLongDisappearClearDays() {
        return longDisappearClearDays;
    }

    public void setLongDisappearClearDays(int longDisappearClearDays) {
        this.longDisappearClearDays = longDisappearClearDays;
    }

    public String getMsgTypes() {
        return msgTypes;
    }

    public void setMsgTypes(String msgTypes) {
        this.msgTypes = msgTypes;
    }

    public String getRemindType() {
        return remindType;
    }

    public void setRemindType(String remindType) {
        this.remindType = remindType;
    }

    public String getCreateBy() {
        return createBy;
    }

    public void setCreateBy(String createBy) {
        this.createBy = createBy;
    }

    public Timestamp getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Timestamp updateTime) {
        this.updateTime = updateTime;
    }
}
