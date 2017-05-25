select
    com.*, danger_person.id, danger_area.brief
from
    comprehensive com
left join h_ser_rq_danger_person danger_person on com.yest_imsi = danger_person.imsi
left join h_ser_rq_danger_area danger_area  on com.yest_area_code = danger_area.area