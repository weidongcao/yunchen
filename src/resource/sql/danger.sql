select com.*, per.id, ar.brief from comprehensive com
left join dangerPerson per on com.yest_imsi = per.imsi
left join dangerArea ar  on com.yest_area_code = ar.area