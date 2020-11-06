#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
DROP TABLE if exists dwd.dwd_Bumblebee_imsi_transaction_detail_tmp;

CREATE TABLE dwd.dwd_Bumblebee_imsi_transaction_detail_tmp
ENGINE=MergeTree
ORDER BY transaction_id AS
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
    t2.channel_contact_name
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
) AS t2 ON t1.merchant_id = t2.merchant_id;

drop table if exists dwd.dwd_Bumblebee_imsi_transaction_detail;

rename table dwd.dwd_Bumblebee_imsi_transaction_detail_tmp to dwd.dwd_Bumblebee_imsi_transaction_detail;
"
