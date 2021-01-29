#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

#一个transaction_id对应多个order_id说明该订单为免费订单,将所有订单聚合为-1
clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dwd.dwd_Einstein_order_imsi_profile_relation_tmp;

CREATE TABLE dwd.dwd_Einstein_order_imsi_profile_relation_tmp
ENGINE = MergeTree
ORDER BY order_id AS
SELECT
    t1.*,
    b.id AS bundle_id
FROM
(
    SELECT
        oipr.order_id,
        if(it.imsi_transaction_id = 0, toInt32OrNull(oipr.transaction_id), it.imsi_transaction_id) AS transaction_id,
        oipr.transaction_id AS transaction_code,
        oipr.bundle_id AS bundle_code,
        oipr.imsi
    FROM
    (
        SELECT
            transaction_id,
            order_id,
            imsi,
            bundle_id
        FROM ods.ods_Einstein_order_imsi_profile_relation
    ) AS oipr
    LEFT JOIN ods.ods_Bumblebee_imsi_transaction AS it ON oipr.transaction_id = it.code
) AS t1
LEFT JOIN
(
    SELECT
        code,
        id
    FROM dim.dim_Bumblebee_bundle
) AS b ON t1.bundle_code = b.code;

drop table if exists dwd.dwd_Einstein_order_imsi_profile_relation;

rename table dwd.dwd_Einstein_order_imsi_profile_relation_tmp to dwd.dwd_Einstein_order_imsi_profile_relation;
"
