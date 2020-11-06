#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

#每天来的数据具有延迟性，所有需要重新group by计算(import_time在此表示该话单的开始时间属于当天)。transaction_id 为0 代表不知道所属哪一个订单，可能为免费流量使用等。
clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dwd.dwd_Bumblebee_imsi_transaction_cdr_raw_tmp;

CREATE TABLE dwd.dwd_Bumblebee_imsi_transaction_cdr_raw_tmp
ENGINE = MergeTree
ORDER BY transaction_id AS
SELECT
    if(transaction_id = 0,-1,transaction_id) as transaction_id,
    imsi,
    bundle_id as bundle_code,
    merchant_id,
    country,
    carrier,
    plmn,
    if(length(location_code)=6,replaceAll(location_code,'F',''),location_code) as location_code,
    min(start_time) as start_time,
    max(end_time) as end_time,
    sum(upload) as total_upload,
    sum(download) as total_download,
    sum(upload + download) AS total_usage,
    max(price) as unit_price,
    sum(upload*price + download*price)/1024/1024/1024 as cost,
    cdr_date
FROM ods.ods_Bumblebee_imsi_transaction_cdr_raw
GROUP BY
    transaction_id,
    imsi,
    bundle_id,
    merchant_id,
    country,
    carrier,
    plmn,
    location_code,
    cdr_date;

drop table if exists dwd.dwd_Bumblebee_imsi_transaction_cdr_raw;

rename table dwd.dwd_Bumblebee_imsi_transaction_cdr_raw_tmp to dwd.dwd_Bumblebee_imsi_transaction_cdr_raw;
"