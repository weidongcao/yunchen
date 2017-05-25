-- 说明：将昨天新增的数据与历史数据进行full join,获取到全部的信息
-- 所有涉及到的表：
-- 昨天数据采集信息表
-- 历史分析表
-- 小区表
-- 高危人群表
-- 高危地区表
-- 手机信息表
-- 设备表

--昨天新增的数据：
-- 从h_scan_ending_improve获取昨天新增的数据，

-- 对昨天新增的数据不同小区的人员出现进行去重：
-- h_scan_ending_improve的sn_code字段与h_machine_info表的machine_id字段进行join
-- h_machine_info表的service_code字段与h_service_info表的service_code字段进行join
-- 获取到小区名，小区ID

-- 获取手机号信息：
-- 与IMSI手机信息表进行join获取手机归属地，归属地代码，手机运营商，归属地电话区号

--获取高危人员信息：
--与高危人员表进行join获取高危人群信息

--获取高危地区信息：
--与高危地区表进行join获取高危地区信息

-- 然后再与历史数据进行full join

-- 获取到的信息：
-- 人员IMSI号（昨天数据采集信息表&历史分析表），
-- 小区名称（小区表&历史分析表），
-- 小区ID（小区表&历史分析表），
-- 高危人群ID（高危人群表），
-- 高危地区名（高危地区表），
-- 权重（历史分析表），
-- 手机号前7位（昨天数据采集信息表）
-- 手机归属地、归属地代码、手机运营商、归属地电话区号（手机信息表）

--  insert overwrite local directory
-- '/opt/caoweidong/yunchen/community/data'
--  row format delimited fields terminated by '\t'

SELECT
    -- 昨天数据人群IMSI号(0)
    info.imsi_code,
    -- 历史数据人员IMSI号(1)
    com.imsi,
    -- 昨天数据小区名(2)
    info.service_name,
    -- 历史数据小区名(3)
    com.service_name,
    -- 昨天数据小区ID(4)
    info.service_code,
    -- 历史数据小区ID(5)
    com.service_code,
    -- 昨天数据高危人群信息(6)
    info.danger_person_id,
    -- 昨天数据高危地区信息(7)
    info.danger_area_brief,
    -- 历史数据人群权重(8)
    com.weight,
    --手机号前7位(9)
    info.phone_num,
    --手机归属地(10)
    info.area_name,
    --归属地代码(11)
    info.area_code,
    --手机运营商(12)
    info.phone_type,
    --归属地电话区号(13)
    info.region
FROM
    (
        SELECT
            improve.imsi_code,      --人群IMSI号
            improve.service_code,   --小区ID
            improve.service_name,   --小区名
            imsiTable.phone_num,    --手机号前7位
            imsiTable.area_name,    --手机归属地
            imsiTable.area_code,    --归属地代码
            imsiTable.phone_type,   --手机运营商
            imsiTable.region,        --归属地电话区号
            danger_person.id as danger_person_id,     --高危人群信息
            danger_area.brief as danger_area_brief       --高危地区信息
        FROM
            -- h_scan_ending_improve昨天新增数据表数据不同小区人群去重
            (select *
            from
                (SELECT
                    service.service_code as service_code, --小区号
                    service.service_name as service_name,    --小区名
                    scan.imsi_code as imsi_code,  --人群IMSI号
                    row_number() over (partition by service.service_code, scan.imsi_code) as num
                FROM
                    h_scan_ending_improve scan  --设备采集信息表
                    -- 筛选后的数据与设备信息表(h_machine_info)进行join
                    LEFT JOIN h_machine_info machine ON scan.sn_code = machine.machine_id
                    -- 设备信息表(h_machine_info)与小区信息表(h_service_info)进行join
                    left join h_service_info service on machine.service_code = service.service_code
                WHERE
                    -- h_scan_ending_improve表只取昨天的数据(capture_time为数据采集时间)
                    substr(scan.capture_time, 0, 10) = '${yesterday}'
                    ) aaa
            where aaa.num = 1) improve
        -- 设备采集信息表(h_scan_ending_improve)与IMSI手机信息表进行join获取手机号前7位，手机归属地，归属地代码，手机运营商，归属地电话区号
        left join h_sys_imsi_key_area imsiTable on improve.imsi_code = imsiTable.imsi_num
        --采集信息表与高危人员表进行join获取高危人群信息
        left join h_ser_rq_danger_person danger_person on improve.imsi_code = danger_person.imsi
        --采集信息表与高危地区表进行join获取高危地区信息
        left join h_ser_rq_danger_area danger_area  on imsiTable.area_code = danger_area.area
    ) as info
-- 上面新增的数据与历史数据中前天的数据进行full join
full outer join (
    select
        service_name,  --小区名
        service_code,   --小区ID
        imsi,   --人群IMSI号
        weight --人群权重
    from
        h_persion_type  --小区人群分析表
    where
        hdate = '${before_yesterday}'    --取前天的数据
    ) as com
on info.imsi_code = com.imsi
and info.service_code = com.service_code