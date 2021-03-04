#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
DROP TABLE IF EXISTS ods.ods_Einstein_registration_count_tmp;

create table if not exists ods.ods_Einstein_registration_count_tmp
Engine=MergeTree
order by id as
select
  0 as id,
  device_id,
  login_time
from mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'registration_count', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c');

DROP TABLE IF EXISTS ods.ods_Einstein_registration_count;

RENAME TABLE ods.ods_Einstein_registration_count_tmp TO ods.ods_Einstein_registration_count;
"