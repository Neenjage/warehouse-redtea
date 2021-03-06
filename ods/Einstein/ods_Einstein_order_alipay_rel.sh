#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
DROP TABLE IF EXISTS ods.ods_Einstein_order_alipay_rel_tmp;

CREATE TABLE ods.ods_Einstein_order_alipay_rel_tmp
ENGINE = MergeTree
ORDER BY id AS
SELECT 
*
FROM mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'order_alipay_rel', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c');

DROP TABLE IF EXISTS ods.ods_Einstein_order_alipay_rel;

RENAME TABLE ods.ods_Einstein_order_alipay_rel_tmp TO ods.ods_Einstein_order_alipay_rel;
"