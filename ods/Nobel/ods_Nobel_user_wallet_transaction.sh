#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists ods.ods_Nobel_user_wallet_transaction_tmp;

create table ods.ods_Nobel_user_wallet_transaction_tmp
Engine=MergeTree
order by id as
select
*
from
mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'user_wallet_transaction', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13');

drop table if exists ods.ods_Nobel_user_wallet_transaction;

rename table ods.ods_Nobel_user_wallet_transaction_tmp to ods.ods_Nobel_user_wallet_transaction;
"
