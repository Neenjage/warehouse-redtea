#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

#取每天最新的数据
clickhouse-client --user $user --password $password --multiquery --multiline -q"
create table if not exists ods.ods_Bell_imsi_resource_tmp
ENGINE=MergeTree
ORDER BY id AS
select * from mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Bell', 'imsi_resource', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table ods.ods_Bell_imsi_resource"


clickhouse-client --user $user --password $password --multiquery --multiline -q"
rename table ods.ods_Bell_imsi_resource_tmp to ods.ods_Bell_imsi_resource"


