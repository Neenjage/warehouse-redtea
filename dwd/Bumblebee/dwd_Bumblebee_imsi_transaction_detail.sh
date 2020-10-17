#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE if not exists dwd.dwd_Bumblebee_imsi_transaction_detail
(
    transaction_id Int32,
    imsi Nullable(String),
    iccid Nullable(String),
    bundle_id Nullable(String),
    imsi_profile_id Nullable(Int32),
    transaction_code String,
    transaction_status Nullable(String),
    merchant_id Nullable(Int32),
    merchant_code Nullable(String),
    merchant_name Nullable(String),
    merchant_status Nullable(String),
    channel_id Nullable(Int32),
    channel_code Nullable(String),
    channel_name Nullable(String),
    channel_country Nullable(String),
    channel_contact_name Nullable(String),
    import_time Date
)
ENGINE = MergeTree
ORDER BY transaction_id
SETTINGS index_granularity = 8192;
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
ALTER TABLE dwd.dwd_Bumblebee_imsi_transaction_detail delete where import_time = '$import_time'
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
INSERT INTO TABLE dwd.dwd_Bumblebee_imsi_transaction_detail
SELECT
    t1.transaction_id,
    t1.imsi,
    t1.iccid,
    t1.bundle_id,
    t1.imsi_profile_id,
    t1.transaction_code,
    t1.transaction_status,
    t1.merchant_id,
    t2.merchant_code,
    t2.merchant_name,
    t2.merchant_status,
    t2.channel_id,
    t2.channel_code,
    t2.channel_name,
    t2.channel_country,
    t2.channel_contact_name,
    '$import_time'
FROM
(
    SELECT
        it.imsi_transaction_id AS transaction_id,
        it.imsi,
        it.bundle_id,
        it.imsi_profile_id,
        it.code AS transaction_code,
        it.status AS transaction_status,
        it.merchant_id,
        ip.iccid
    FROM ods.ods_Bumblebee_imsi_transaction it
    left join ods.ods_Bumblebee_imsi_profile ip on it.imsi = ip.imsi
    WHERE it.import_time = '$import_time'
) AS t1
LEFT JOIN
(
    SELECT
        merchant.id AS merchant_id,
        merchant.code AS merchant_code,
        merchant.name AS merchant_name,
        merchant.status AS merchant_status,
        merchant.channel_id AS channel_id,
        channel.channel_id AS channel_code,
        channel.channel_name AS channel_name,
        channel.channel_country AS channel_country,
        channel.channel_contact_name AS channel_contact_name
    FROM
    (
        SELECT
            id,
            code,
            name,
            status,
            channel_id
        FROM dim.dim_Bumblebee_merchant
        WHERE import_time = '$import_time'
    ) AS merchant
    LEFT JOIN
    (
        SELECT
            id,
            channel_id,
            channel_name,
            channel_country,
            channel_contact_name
        FROM dim.dim_Bumblebee_channel
        WHERE import_time = '$import_time'
    ) AS channel ON merchant.channel_id = channel.id
) AS t2 ON t1.merchant_id = t2.merchant_id
"





