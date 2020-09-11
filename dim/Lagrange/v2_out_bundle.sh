#!/bin/bash

clickhouse-client -u$1 --multiquery -q"
INSERT INTO table ods_Lagrange_Bayer.v2_out_bundle
(data_plan_id, bundle_id, mcc_list, sort_num, status, create_time, update_time, provision_mcc_list, import_time)
SELECT data_plan_id, bundle_id, mcc_list, sort_num, status, create_time, update_time, provision_mcc_list,today() from
mysql('lagrange-redteago-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Lagrange', 'v2_out_bundle', 'redtea-ro', 'Ahxee7auzaeGhoo5phien4uTah1pahja4');
"
