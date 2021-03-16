#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
DROP TABLE IF EXISTS ods.ods_Einstein_i18n_resource_tmp;

CREATE TABLE ods.ods_Einstein_i18n_resource_tmp
ENGINE = MergeTree
ORDER BY resource_key AS
SELECT 
  *
FROM mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'i18n_resource', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c');

DROP TABLE IF EXISTS ods.ods_Einstein_i18n_resource;

RENAME TABLE ods.ods_Einstein_i18n_resource_tmp TO ods.ods_Einstein_i18n_resource;
"