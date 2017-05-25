-- insert overwrite local directory
-- '/opt/caoweidong/script/hive/data/keyarea'
-- row format delimited fields terminated by '\t'
select
    imsi_code,
    sn_code,
    count(1) as mac_appear_times
from
    h_scan_ending_improve
where
    ${collision_condition}
group by
    sn_code,
    imsi_code
