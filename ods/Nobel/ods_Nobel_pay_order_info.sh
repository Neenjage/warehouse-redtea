#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists ods.ods_Nobel_pay_order_info_tmp;

create table ods.ods_Nobel_pay_order_info_tmp
Engine=MergeTree
order by id as
select
*
from
mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'pay_order_info', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13');

drop table if exists ods.ods_Nobel_pay_order_info;

rename table ods.ods_Nobel_pay_order_info_tmp to ods.ods_Nobel_pay_order_info;
"