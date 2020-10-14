#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE ods.ods_Einstein_unified_identification_temp
ENGINE = MergeTree
ORDER BY uid AS
SELECT *
FROM mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'unified_identification', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c')
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
DROP TABLE ods.ods_Einstein_unified_identification;
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
RENAME TABLE ods.ods_Einstein_unified_identification_temp TO ods.ods_Einstein_unified_identification;
"

