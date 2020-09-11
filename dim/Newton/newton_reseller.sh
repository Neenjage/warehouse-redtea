#!/bin/bash
clickhouse-client -u$1 --multiquery -q"
INSERT INTO ods_Newton.newton_reseller
(id, name, remark, ws_url, access_key, secret_key, import_time)
SELECT id, name, remark, ws_url, access_key, secret_key, today() from 
mysql('ro-newton-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Newton', 'reseller', 'redtea-ro', 'TecirEk8ph2jukapH83jcefaqAfa4Gpcg')
"
