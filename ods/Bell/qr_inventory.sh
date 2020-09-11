#!/bin/bash
date=`date +%Y-%m-%d`
ydate=`date --date='1 day ago' +%Y-%m-%d`
today="$date 16:00:00"
yesterday="$ydate 16:00:00"

clickhouse-client -u$1 --multiquery -q"
create table ods_Bell_Nobel.qr_inventory_tmp
ENGINE=MergeTree 
ORDER BY id AS
select * from mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Bell', 'qr_inventory', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
"
clickhouse-client -u$1 --multiquery -q"drop table ods_Bell_Nobel.qr_inventory"
clickhouse-client -u$1 --multiquery -q"rename table ods_Bell_Nobel.qr_inventory_tmp to ods_Bell_Nobel.qr_inventory"



