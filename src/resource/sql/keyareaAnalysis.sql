insert overwrite local directory
'/opt/caoweidong/script/hive/data/keyarea'
 row format delimited fields terminated by '\t'

SELECT
    improve.imsi_code,
    improve.phone_num,
    improve.area_name,
    improve.area_code,
    improve.sn_code,
    improve.capture_time,
    service.service_code,
    service.service_name,

    com.imsi
    com.service_name
    com.service_code
    com.
    com.
    com.appear_days,
    com.appear_times,
    com.people_type,
    com.identification
FROM
    yuncai.improve_phone improve
    -- 筛选后的数据与设备信息表(h_machine_info)进行join
    LEFT JOIN yuncai.h_machine_info machine ON improve.sn_code = machine.machine_id
    -- 设备信息表(h_machine_info)与小区信息表(h_service_info)进行join
    left join yuncai.h_service_info service on machine.service_code = service.service_code
full outer join (
    select
        service_name,  --小区名
        service_code,   --小区ID
        machine_id,    --设备ID
        imsi,   --人群IMSI号
        appear_days,
        appear_times,
        people_type,
        identification
    from
        yuncai.h_keyarea_analysis  --小区人群分析表
    where
        hdate = '2017-03-08_18:30'    --取前天的数据
    ) as com
on improve.imsi_code = com.imsi
and com.service_code = service.service_code;