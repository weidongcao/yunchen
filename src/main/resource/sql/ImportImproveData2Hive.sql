insert into table buffer_ending_improve partition(ds)
select
    equipment_mac,
    imsi_code,
    capture_time,
    operator_type,
    sn_code,
    longitude,
    latitude,
    first_time,
    last_time,
    dist,
    import_time,
    phone_area,
    phone_num ,
    to_date(capture_time) as ds
from
    ${tablename}




