#!/bin/bash
clickhouse-client -udefault --multiquery -q"
CREATE TABLE IF NOT EXISTS ods.ods_Einstein_device_tmp
ENGINE = MergeTree
ORDER BY device_id AS
SELECT *
FROM mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'device', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c')
"

clickhouse-client -u$1 --multiquery -q"
DROP TABLE ods.ods_Einstein_device;
"


clickhouse-client -u$1 --multiquery -q"
RENAME TABLE ods.ods_Einstein_device_tmp TO ods.ods_Einstein_device
"
