--insert overwrite local directory
--'/opt/caoweidong/data/keyarea'
--row format delimited fields terminated by '\t'
--collection items terminated BY ','
--map keys terminated BY ':'
select
    service_name,
    service_code,
    concat_ws(' ', ds, hr) as stat_date, --统计时间
    count(1) as total_people,
    count(case when (substring(bin(if(people_type < 8, people_type + 8, people_type)), 2, 1) = '1') then 1 else null end) as suspicious_people, --可疑人群数,
    count(case when (substring(bin(if(people_type < 8, people_type + 8, people_type)), 3, 1) = '1') then 1 else null end) as attention_people, --高危人群数,
    count(case when (substring(bin(if(people_type < 8, people_type + 8, people_type)), 4, 1) = '1') then 1 else null end) as danger_area_people --高危地区人群数
from
    buffer_emphasis_analysis
where
    unix_timestamp(last_capture_time, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(concat_ws(' ', ${ds}, ${hr}), 'yyyy-MM-dd HH') < 3600
group by
    concat_ws(' ', ${ds}, ${hr}), service_code, service_name;

