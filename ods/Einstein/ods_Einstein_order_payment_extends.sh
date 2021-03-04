#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
DROP TABLE IF EXISTS ods.ods_Einstein_order_payment_extends_tmp;

CREATE TABLE ods.ods_Einstein_order_payment_extends_tmp
ENGINE = MergeTree
ORDER BY id AS
SELECT 
    id,
    order_id,
    payment_time,
    refund_time
FROM mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'order_payment_extends', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c');

DROP TABLE IF EXISTS ods.ods_Einstein_order_payment_extends;

RENAME TABLE ods.ods_Einstein_order_payment_extends_tmp TO ods.ods_Einstein_order_payment_extends;
"