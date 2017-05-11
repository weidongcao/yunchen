select
    --采集数据IMSI号(0)
    emphasis.imsi_code,
    --采集数据手机号前7位(1)
    emphasis.phone_num,
    --采集数据手机归属地名(2)
    emphasis.area_name,
    --采集数据手机归属地代码(3)
    emphasis.area_code,
    --数据采集时间(4)
    emphasis.capture_time,
    --采集数据小区号(5)
    emphasis.service_code,
    --采集数据小区名(6)
    emphasis.service_name,
    --手机运营商(7)
    emphasis.phone_type,
    --高危地区(8)
    emphasis.area_brief,
    --高危人群(9)
    emphasis.persion_brief,
    --高危人群等级(10)
    emphasis.erank,
    --高危人群类型(11)
    emphasis.etype,
    --可疑人群计算周期(12)
    emphasis.doubtful_period,
    --可疑人群出现天数(13)
    emphasis.doubtful_days,
    --可疑人群出现次数(14)
    emphasis.doubtful_times,

    --分析数据IMSI号(15)
    com.imsi_code,
    --分析数据小区名(16)
    com.service_name,
    --分析数据小区号(17)
    com.service_code,
    --分析数据人员出现天数(18)
    com.appear_days,
    --分析数据人员出现次数(19)
    com.appear_times,
    --分析数据人员类型(20)
    com.people_type,
    --高危人群类型(21)
    com.danger_person_type,
    --高危人群等级(22)
    com.danger_person_rank,
    --高危人群备注(23)
    com.danger_person_brief,
    --高危地区备注(24)
    com.dnager_area_brief,
    --分析数据人员标识(25)
    com.identification,
    --最近一次采集时间(26)
    com.hdate
from
    (select * from temp_emphasis where capture_time >= "${preCountTime}" and capture_time < "${curCountTime}") emphasis
full outer join (
    select
        service_name,  --小区名
        service_code,   --小区ID
        imsi_code,   --人群IMSI号
        hdate,        --最近一次采集时间
        appear_days,  --出现天数
        appear_times, --出现次数
        people_type,  --人员类型
        danger_person_type, --高危人群类型
        danger_person_rank,  --高危人群等级
        danger_person_brief, --高危人群备注
        dnager_area_brief,  --高危地区备注
        identification  --标识符
    from
        buffer_emphasis_analysis  --小区人群分析表
    where
        ds = "${preCountDate}" and hr = "${hr}"    --取之前的数据
    ) as com
on emphasis.imsi_code = com.imsi_code
and com.service_code = emphasis.service_code



