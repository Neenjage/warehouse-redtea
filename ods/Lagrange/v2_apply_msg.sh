#!/bin/bash

clickhouse-client -u$1 --multiquery -q"
INSERT INTO table ods_Lagrange_Bayer.v2_apply_msg
(id, ota_transaction_id, order_transaction_id, cid, mcc,localtime, create_time,direct,import_time)
SELECT id, ota_transaction_id, order_transaction_id, cid, mcc,localtime, create_time,direct,today() from
mysql('lagrange-redteago-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Lagrange', 'v2_apply_msg', 'redtea-ro', 'Ahxee7auzaeGhoo5phien4uTah1pahja4')
where id > (select max(id) max_c from ods_Lagrange_Bayer.v2_apply_msg);
"
