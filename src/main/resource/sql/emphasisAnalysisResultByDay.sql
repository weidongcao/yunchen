select
    inf.service_name,
    inf.service_code,
    inf.stat_date,
    count(1) as total_people,
    null as suspicious_people,
    sum(case when ((inf.danger_person_brief is not NULL) and (inf.danger_person_brief != '') and (inf.danger_person_brief != 'NULL')) then 1 else 0 end) as attention_people, --高危人群数,
    sum(case when ((inf.danger_area_brief is not NULL) and (inf.danger_area_brief != '') and (inf.danger_area_brief != 'NULL')) then 1 else 0 end) as danger_area_people --高危地区人群数
from
    (select
        distinct service_code, service_name, imsi_code, danger_person_brief, danger_area_brief, ds as stat_date
    from
        buffer_emphasis_analysis
    where
        ds = '${ds}'
        and (substring(identification, 2, 1) != '0')) inf
group by
    inf.service_code, inf.service_name, inf.stat_date