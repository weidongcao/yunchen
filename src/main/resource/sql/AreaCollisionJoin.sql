-- insert overwrite local directory
-- '/opt/caoweidong/script/hive/data/keyarea'
-- row format delimited fields terminated by '\t'
select
    imsi_area.imsi_code,  --IMSI号
    phone.phone_type,    --手机运营商
    phone.area_name,     --手机归属地
    persion.brief, --高危人群
    imsi_area.area_appear_time   --同一采集设备下出现次数
from
    imsi_area
left join h_sys_phone_to_area    phone    on imsi_area.phone_num = phone.phone_num
left join h_ser_rq_danger_person persion on imsi_area.imsi_code = persion.imsi
