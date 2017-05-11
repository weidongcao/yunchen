--improve表为Spark临时注册的表，它包含设备采集信息及由IMSI号生成的手机号前7位
--improve表与手机信息表(h_sys_phone_to_area)join获取手机归属地等信息
--improve表与高危人群表(h_ser_rq_danger_person)join获取高危人群信息
--improve表与高危地区表(h_ser_rq_danger_area)join获取高危地区信息
--improve表与设备表(h_machine_info)join获取到小区号
--improve表与小区信息表(h_service_info)join获取到小区名
--improve表与重点区域配置表(h_ser_rq_emphasis_area_config)join获取到重点区域的配置
--这样就获取到了采集数据相关的小区信息、高危人群信息、高危地区信息
--然后与之前的历数分析数据进行full join获取到统计周期内全部的人员信息
--然后处理sql的输出结果，分析出当前时间重点区域的人员情况

--insert测试打开
--insert overwrite local directory
--'/opt/caoweidong/script/hive/data/keyarea'
-- row format delimited fields terminated by '\t'
select
    --采集数据IMSI号(0)
    improve.imsi_code,
    --采集数据手机号前7位(1)
    improve.phone_num,
    --采集数据手机归属地名(2)
    phone.area_name,
    --采集数据手机归属地代码(3)
    phone.area_code,
    --数据采集时间(4)
    improve.capture_time,
    --采集数据小区号(5)
    service.service_code,
    --采集数据小区名(6)
    service.service_name,
    --手机运营商(7)
    phone.phone_type,
    --高危地区(8)
    danger_area.brief,
    --高危人群(9)
    danger_person.brief,
    --高危人群等级(10)
    danger_person.rank,
    --高危人群类型(11)
    danger_person.type,
    --可疑人群计算周期(12)
    emphasis.doubtful_period,
    --可疑人群出现天数(13)
    emphasis.doubtful_days,
    --可疑人群出现次数(14)
    emphasis.doubtful_times,

    --分析数据IMSI号(15)
    com.imsi_code,
    --分析数据小区名(16)
    com.service_name,
    --分析数据小区号(17)
    com.service_code,
    --分析数据人员出现天数(18)
    com.appear_days,
    --分析数据人员出现次数(19)
    com.appear_times,
    --分析数据人员类型(20)
    com.people_type,
    --高危人群类型(21)
    com.danger_person_type,
    --高危人群等级(22)
    com.danger_person_rank,
    --高危人群备注(23)
    com.danger_person_brief,
    --高危地区备注(24)
    com.dnager_area_brief,
    --分析数据人员标识(25)
    com.identification,
    --最近一次采集时间(26)
    com.hdate
from
    improve
    --与手机号信息表join获取手机号归属地
    left join h_sys_phone_to_area phone on improve.phone_num = phone.phone_num
    --与高危人群表join获取高危人群信息
    left join h_ser_rq_danger_person danger_person on improve.imsi_code = danger_person.imsi
    --与高危地区表join获取高估地区信息
    left join h_ser_rq_danger_area danger_area on phone.area_code = danger_area.area
    -- 筛选后的数据与设备信息表(h_machine_info)进行join
    LEFT join h_machine_info machine ON improve.sn_code = machine.machine_id
    -- 设备信息表(h_machine_info)与小区信息表(h_service_info)进行join
    left join h_service_info service on machine.service_code = service.service_code
    --重点区域配置表
    left join h_ser_rq_emphasis_area_config emphasis on emphasis.service_code = service.service_code
full outer join (
    select
        service_name,  --小区名
        service_code,   --小区ID
        imsi_code,   --人群IMSI号
        hdate,        --最近一次采集时间
        appear_days,  --出现天数
        appear_times, --出现次数
        people_type,  --人员类型
        danger_person_type, --高危人群类型
        danger_person_rank,  --高危人群等级
        danger_person_brief, --高危人群备注
        dnager_area_brief,  --高危地区备注
        identification  --标识符
    from
        h_emphasis_analysis  --小区人群分析表
    where
        to_date(hdate) = '2017-03-08'    --取前天的数据
    ) as com
on improve.imsi_code = com.imsi_code
and com.service_code = service.service_code



