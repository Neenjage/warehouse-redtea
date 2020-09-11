#!/bin/bash

clickhouse-client -u$1 --multiquery -q"
INSERT INTO table ods_Lagrange_Bayer.jt_online_history
(id, imsi, vlr, create_time,import_time)
SELECT id, imsi, vlr, create_time,today() from
mysql('lagrange-redteago-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Lagrange', 'jt_online_history', 'redtea-ro', 'Ahxee7auzaeGhoo5phien4uTah1pahja4')
where id > (select max(id) max_c from ods_Lagrange_Bayer.jt_online_history);
"
