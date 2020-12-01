#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists ods.ods_Einstein_wxpay_response_tmp;

create table ods.ods_Einstein_wxpay_response_tmp
Engine=MergeTree
order by out_trade_no as
select
*
from
mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'wxpay_response', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c');

drop table if exists ods.ods_Einstein_wxpay_response;

rename table ods.ods_Einstein_wxpay_response_tmp to ods.ods_Einstein_wxpay_response;
"
