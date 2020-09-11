#!/bin/bash

clickhouse-client -u$1 --multiquery -q"
INSERT INTO table ods_Lagrange_Bayer.Bayer_data_plan
(id, data_plan_inner_name, price, sort_no, discount_price, out_data_plan_id, timezone_fix, picture_s, picture_b, picture_flag, status, create_time, last_update_time, day_count, import_time)
SELECT id, data_plan_inner_name, price, sort_no, discount_price, out_data_plan_id, timezone_fix, picture_s, picture_b, picture_flag, status, create_time, last_update_time, day_count,today() from
mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Bayer', 'data_plan', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
"
