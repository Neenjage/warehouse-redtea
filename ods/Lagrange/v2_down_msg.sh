#!/bin/bash
date=`date +%Y-%m-%d`
ydate=`date --date='1 day ago' +%Y-%m-%d`
today="$date 16:00:00"
yesterday="$ydate 16:00:00"

clickhouse-client -u$1 --multiquery -q"
create table ods_Lagrange_Bayer.v2_down_msg_tmp
ENGINE=MergeTree 
ORDER BY id AS
select * from mysql('lagrange-redteago-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Lagrange', 'v2_down_msg', 'redtea-ro', 'Ahxee7auzaeGhoo5phien4uTah1pahja4')
"
clickhouse-client -u$1 --multiquery -q"drop table ods_Lagrange_Bayer.v2_down_msg"
clickhouse-client -u$1 --multiquery -q"rename table ods_Lagrange_Bayer.v2_down_msg_tmp to ods_Lagrange_Bayer.v2_down_msg"



