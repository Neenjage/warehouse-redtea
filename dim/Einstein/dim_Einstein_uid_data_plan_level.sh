#!/bin/bash

source  /home/ops/warehouse-redtea/config/config.sh


clickhouse-client --user $user --password $password --multiquery --multiline -q"
create table if not exists dim.dim_Einstein_uid_data_plan_level
Engine=MergeTree
order by uid_level as
select * from
mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'uid_data_plan_level', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c');
"
