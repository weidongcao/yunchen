package com.rainsoft.domain.java;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * Created by Administrator on 2017-04-25.
 */
public class EmphasisConfig implements Serializable {
    public int id;

    public String serviceCode;

    public int doubtful_period = 3;

    public int doubtful_days = 2;

    public int doubtful_times = 5;

    public String msg_types;

    public String remind_type;

    public String remind_begin_time;

    public String remind_end_time;

    public int create_by;

    public Timestamp update_time;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getServiceCode() {
        return serviceCode;
    }

    public void setServiceCode(String serviceCode) {
        this.serviceCode = serviceCode;
    }

    public int getDoubtful_period() {
        return doubtful_period;
    }

    public void setDoubtful_period(int doubtful_period) {
        this.doubtful_period = doubtful_period;
    }

    public int getDoubtful_days() {
        return doubtful_days;
    }

    public void setDoubtful_days(int doubtful_days) {
        this.doubtful_days = doubtful_days;
    }

    public int getDoubtful_times() {
        return doubtful_times;
    }

    public void setDoubtful_times(int doubtful_times) {
        this.doubtful_times = doubtful_times;
    }

    public String getMsg_types() {
        return msg_types;
    }

    public void setMsg_types(String msg_types) {
        this.msg_types = msg_types;
    }

    public String getRemind_type() {
        return remind_type;
    }

    public void setRemind_type(String remind_type) {
        this.remind_type = remind_type;
    }

    public String getRemind_begin_time() {
        return remind_begin_time;
    }

    public void setRemind_begin_time(String remind_begin_time) {
        this.remind_begin_time = remind_begin_time;
    }

    public String getRemind_end_time() {
        return remind_end_time;
    }

    public void setRemind_end_time(String remind_end_time) {
        this.remind_end_time = remind_end_time;
    }

    public int getCreate_by() {
        return create_by;
    }

    public void setCreate_by(int create_by) {
        this.create_by = create_by;
    }

    public Timestamp getUpdate_time() {
        return update_time;
    }

    public void setUpdate_time(Timestamp update_time) {
        this.update_time = update_time;
    }
}
