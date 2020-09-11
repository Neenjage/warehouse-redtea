#!/bin/bash
date=`date +%Y-%m-%d`
ydate=`date --date='1 day ago' +%Y-%m-%d`
today="$date 16:00:00"
yesterday="$ydate 16:00:00"

clickhouse-client -u$1 --multiquery -q"
insert into table ods_Lagrange_Bayer.Bayer_data_plan_order
(id, order_id, cid, iccid, out_data_plan_id, out_order_id, start_time, day_count, end_time, data_plan_id, data_plan_timezone_fix, data_plan_price, order_price, status, create_time, last_update_time, data_plan_type)
select * from mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Bayer', 'data_plan_order', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
where last_update_time > (select max(last_update_time) max_c from ods_Lagrange_Bayer.Bayer_data_plan_order)
"

clickhouse-client -u$1 --multiquery -q"
insert into table ods_Lagrange_Bayer.Bayer_data_plan_order
(id, order_id, cid, iccid, out_data_plan_id, out_order_id, start_time, day_count, end_time, data_plan_id, data_plan_timezone_fix, data_plan_price, order_price, status, create_time, last_update_time, data_plan_type)
select id, order_id, cid, iccid, out_data_plan_id, out_order_id, start_time, day_count, end_time, data_plan_id, data_plan_timezone_fix, data_plan_price, order_price, status, create_time, last_update_time, data_plan_type
from (select * from ods_Lagrange_Bayer.Bayer_data_plan_order where last_update_time > '$today') as a 
left join 
( select id from ods_Lagrange_Bayer.Bayer_data_plan_order where create_time < '$today') as b on a.id = b.id
where b.id is null
"
