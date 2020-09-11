#!/bin/bash

clickhouse-client -u$1 --multiquery -q"
INSERT INTO table ods_Lagrange_Bayer.v2_data_plan
(id, name, price, traffic_price, fix_time, keep_time, status, create_time, update_time, days, traffic_detail, apn, carrier, support4g, vendor, import_time)
SELECT id, name, price, traffic_price, fix_time, keep_time, status, create_time, update_time, days, traffic_detail, apn, carrier, support4g, vendor,today() from
mysql('lagrange-redteago-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Lagrange', 'v2_data_plan', 'redtea-ro', 'Ahxee7auzaeGhoo5phien4uTah1pahja4');
"
