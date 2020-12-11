#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dwd.dwd_Bethune_top_up_order_detail_tmp;

CREATE TABLE dwd.dwd_Bethune_top_up_order_detail_tmp
ENGINE = MergeTree
ORDER BY user_id AS
SELECT
    t1.*,
    payment_order.status AS payment_status,
    payment_order.update_time AS payment_time
FROM
(
    SELECT
        top_up_order.id,
        top_up_order.user_id,
        top_up_order.order_no,
        top_up_order.top_up_mobile,
        top_up_order.payment_mode,
        top_up_order.pay_status,
        top_up_order.top_up_type,
        top_up_order.amount,
        top_up_order.product_id,
        top_up_order.product_name,
        top_up_order.create_time,
        top_up_order.update_time,
        payment_order_info.payment_order_id
    FROM ods.ods_Bethune_top_up_order AS top_up_order
    LEFT JOIN ods.ods_Bethune_payment_order_info AS payment_order_info ON top_up_order.order_no = payment_order_info.order_id
) AS t1
LEFT JOIN ods.ods_Mammon_payment_order AS payment_order ON t1.payment_order_id = payment_order.order_id;

drop table if exists dwd.dwd_Bethune_top_up_order_detail;

rename table dwd.dwd_Bethune_top_up_order_detail_tmp to dwd.dwd_Bethune_top_up_order_detail;
"
