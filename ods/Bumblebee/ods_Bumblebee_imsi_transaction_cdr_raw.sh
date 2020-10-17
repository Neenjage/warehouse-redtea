#!/bin/bash

#话单详情表

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE if not exists ods.ods_Bumblebee_imsi_transaction_cdr_raw
(
    imsi Nullable(String),
    iccid Nullable(String),
    start_time Nullable(DateTime),
    end_time Nullable(DateTime),
    data_type String,
    upload Nullable(Int64),
    download Nullable(Int64),
    generate_time DateTime,
    location_code Nullable(String),
    seq_key Int64,
    carrier Nullable(String),
    file_name Nullable(String),
    file_line Nullable(Int32),
    valid Int8,
    transaction_id Int32,
    bundle_id Nullable(String),
    merchant_id Nullable(Int32),
    is_test Nullable(Int32),
    country Nullable(String),
    price Nullable(Float32),
    plmn Nullable(String),
    cdr_date Date
)
ENGINE = MergeTree
ORDER BY seq_key
SETTINGS index_granularity = 8192;

INSERT INTO table ods.ods_Bumblebee_imsi_transaction_cdr_raw
SELECT
    imsi ,
    iccid ,
    start_time ,
    end_time ,
    data_type ,
    upload ,
    download ,
    generate_time ,
    location_code ,
    seq_key ,
    carrier ,
    file_name ,
    file_line,
    valid ,
    transaction_id ,
    bundle_id ,
    merchant_id ,
    is_test ,
    country ,
    price ,
    plmn ,
    toDate(start_time)
FROM mysql('db-redtea-darwin.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'darwin', 'imsi_transaction_cdr_raw', 'darwin', 'QituhqJvF2QlNw01hdjxr0wqUkkD8YmCd')
WHERE seq_key >
(
    SELECT max(seq_key)
    FROM ods.ods_Bumblebee_imsi_transaction_cdr_raw
);
"


