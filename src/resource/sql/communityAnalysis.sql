-- 说明：将昨天新增的数据与历史数据进行full join
-- 获取到人员IMSI号，小区名称， 小区ID，设备ID，权重，日期

-- 从h_scan_ending_improve获取昨天新增的数据，并对昨天新增的数据不同小区的人员出现进行去重
-- 从h_machine_info获取设备信息
-- 从h_service_info获取小区信息
-- h_scan_ending_improve的sn_code字段与h_machine_info表的machine_id字段进行join
-- h_machine_info表的service_code字段与h_service_info表的service_code字段进行join

-- 三个表进行Join，获取不同小区不重复的人员IMSI号，小区名称，设备ID，小区ID
-- 然后再与历史数据进行full join

-- 切换数据库实例

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
    -- 昨天数据设备ID(6)
    info.machine_id,
    -- 历史数据设备ID(7)
    com.machine_id,
    -- 历史数据人群权重(8)
    com.weight
FROM
    -- h_scan_ending_improve， h_machine_info，h_service_info三个表join
    (
        SELECT
            improve.imsi_code,  --人群IMSI号
            machine.machine_id,  --设备ID
            service.service_code,  --小区ID
            service.service_name  --小区名
        FROM
            -- h_scan_ending_improve昨天新增数据表数据不同小区人群去重
            (
                SELECT
                    sn_code,  --设备ID
                    imsi_code  --人群IMSI号
                FROM
                    yuncai.h_scan_ending_improve  --设备采集信息表
                WHERE
                    -- h_scan_ending_improve表只取昨天的数据(capture_time为数据采集时间)
                    substr(capture_time, 0, 10) = '${yesterday}'
                    -- 通过group by设备ID和人群IMSI号对 不同小区的人群进行去重
                GROUP BY
                    sn_code,  --设备ID
                    imsi_code  --人群IMSI号
            ) improve
        -- 筛选后的数据与设备信息表(h_machine_info)进行join
        LEFT JOIN yuncai.h_machine_info machine ON improve.sn_code = machine.machine_id
        -- 设备信息表(h_machine_info)与小区信息表(h_service_info)进行join
        left join yuncai.h_service_info service on machine.service_code = service.service_code
    ) as info
-- 上面新增的数据与历史数据中前天的数据进行full join
full outer join (
    select
        service_name,  --小区名
        service_code,   --小区ID
        machine_id,    --设备ID
        imsi,   --人群IMSI号
        weight --人群权重
    from
        yuncai.h_persion_type  --小区人群分析表
    where
        hdate = '${before_yesterday}'    --取前天的数据
    ) as com
on info.imsi_code = com.imsi
and com.service_code = info.service_code;