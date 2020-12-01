#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists ods.ods_Mammon_payment_order_tmp;

create table ods.ods_Mammon_payment_order_tmp
Engine=MergeTree
order by id
as
select
 *
from
mysql('nucleus-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Mammon', 'payment_order', 'yu.zhou', 'YPRgNmZYzgMcrLP5SgLavpV5r47KvJVtb');

drop table if exists ods.ods_Mammon_payment_order;

rename table ods.ods_Mammon_payment_order_tmp to ods.ods_Mammon_payment_order;
"