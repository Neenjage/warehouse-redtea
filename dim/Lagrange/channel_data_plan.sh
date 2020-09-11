#!/bin/bash

clickhouse-client -u$1 --multiquery -q"
INSERT INTO table ods_Lagrange_Bayer.channel_data_plan
(id, app_id, data_plan_id, status, package_price, traffic_price, create_time, update_time, traffic_rate, payment_type,import_time)
SELECT id, app_id, data_plan_id, status, package_price, traffic_price, create_time, update_time, traffic_rate, payment_type,today() from
mysql('lagrange-redteago-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Lagrange', 'channel_data_plan', 'redtea-ro', 'Ahxee7auzaeGhoo5phien4uTah1pahja4');
"
