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
    danger_person.danger_info,
    --高危人群等级(10)
    danger_person.erank,
    --高危人群类型(11)
    danger_person.etype,
    --可疑人群计算周期(12)
    emphasis_conf.doubtful_period,
    --可疑人群出现天数(13)
    emphasis_conf.doubtful_days,
    --可疑人群出现次数(14)
    emphasis_conf.doubtful_times,

    --分析数据IMSI号(15)
    emphasis.imsi_code,
    --分析数据小区名(16)
    emphasis.service_name,
    --分析数据小区号(17)
    emphasis.service_code,
    --分析数据人员出现天数(18)
    emphasis.appear_days,
    --分析数据人员出现次数(19)
    emphasis.appear_times,
    --分析数据人员类型(20)
    emphasis.people_type,
    --高危人群类型(21)
    emphasis.danger_person_type,
    --高危人群等级(22)
    emphasis.danger_person_rank,
    --高危人群备注(23)
    emphasis.danger_person_brief,
    --高危地区备注(24)
    emphasis.danger_area_brief,
    --分析数据人员标识(25)
    emphasis.identification,
    --最近一次采集时间(26)
    emphasis.last_capture_time
from
    (select equipment_mac, imsi_code, phone_num, capture_time, operator_type, sn_code from h_scan_ending_improve where to_date(capture_time) = '2017-03-09') improve
    --与手机号信息表join获取手机号归属地
    left join h_sys_phone_to_area phone on improve.phone_num = phone.phone_num
    --与高危人群表join获取高危人群信息
    left join
        (select
          person.type as etype,     --高危人群类型
          person.imsi as imsi_code, --高危人群IMSI号
          person.rank as erank,     --高危人群排名
          collect_set(gr.name) as danger_info   --高危人群分组信息其他组织形式为:["分组1","分组2"]
        from
            h_ser_rq_danger_person person --高危人群表
        --与高危人群与分组关系表进行join
        left join h_ser_rq_danger_person_group_rel rel on  person.id = rel.person_id
        --高危人群与分组关系表与高危分组表进行join
        left join h_ser_rq_danger_person_group gr on rel.group_id = gr.id
        --按高危人员进行分组,因为存在一个高危人员对应多个高危分组的情况
        group by person.type,person.imsi,person.rank) danger_person
    on improve.imsi_code = danger_person.imsi_code

    --与高危地区表join获取高危地区信息
    left join h_ser_rq_danger_area danger_area on phone.area_code = danger_area.area
    -- 筛选后的数据与设备信息表(h_machine_info)进行join
    LEFT join h_machine_info machine ON improve.sn_code = machine.machine_id
    -- 设备信息表(h_machine_info)与小区信息表(h_service_info)进行join
    left join h_service_info service on machine.service_code = service.service_code
    --重点区域配置表
    left join h_ser_rq_emphasis_area_config emphasis_conf on emphasis_conf.service_code = service.service_code
full outer join (
    select
        service_name,  --小区名
        service_code,   --小区ID
        imsi_code,   --人群IMSI号
        last_capture_time,        --最近一次采集时间
        appear_days,  --出现天数
        appear_times, --出现次数
        people_type,  --人员类型
        danger_person_type, --高危人群类型
        danger_person_rank,  --高危人群等级
        danger_person_brief, --高危人群备注
        danger_area_brief,  --高危地区备注
        identification  --标识符
    from
        buffer_emphasis_analysis  --小区人群分析表
    where
        ds = "2017-03-08" and hr = "23"    --取前天的数据
    ) as emphasis
on improve.imsi_code = emphasis.imsi_code
and emphasis.service_code = service.service_code