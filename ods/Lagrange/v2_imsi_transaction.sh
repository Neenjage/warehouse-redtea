#!/bin/bash

clickhouse-client -u$1 --multiquery -q"
INSERT INTO table ods_Lagrange_Bayer.v2_imsi_transaction
(order_transaction_id, imsi, out_transaction_id, create_time)
SELECT order_transaction_id, imsi, out_transaction_id, create_time from
mysql('lagrange-redteago-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Lagrange', 'v2_imsi_transaction', 'redtea-ro', 'Ahxee7auzaeGhoo5phien4uTah1pahja4')
where create_time > (select max(create_time) max_c from ods_Lagrange_Bayer.v2_imsi_transaction);
"
