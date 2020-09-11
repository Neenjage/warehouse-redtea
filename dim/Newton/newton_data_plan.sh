#!/bin/bash
clickhouse-client -u$1 --multiquery -q"
INSERT INTO ods_Newton.newton_data_plan
(data_plan_id, name, reseller_id, price, status, update_time, duration, data_volume, code, apn, mcc_mnc, pool_size, expiration_days, hplmn, rplmn, fplmn, rat, ehplmns, oplmns, description, type, display_name, bundle_enabled, bundle_id, is_synchronized, spn, import_time)
SELECT data_plan_id, name, reseller_id, price, status, update_time, duration, data_volume, code, apn, mcc_mnc, pool_size, expiration_days, hplmn, rplmn, fplmn, rat, ehplmns, oplmns, description, type, display_name, bundle_enabled, bundle_id, is_synchronized, spn, today() from 
mysql('ro-newton-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Newton', 'data_plan', 'redtea-ro', 'TecirEk8ph2jukapH83jcefaqAfa4Gpcg')
"
