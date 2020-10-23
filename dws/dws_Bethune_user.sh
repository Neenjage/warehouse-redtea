#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password '' --multiquery --multiline -q"
drop table if exists dws.dws_Bethune_user_tmp;

CREATE TABLE dws.dws_Bethune_user_tmp
ENGINE = MergeTree
ORDER BY user_id AS
SELECT
    t1.*,
    if(isNull(topup_order.total_topup_amount), 0, topup_order.total_topup_amount) AS total_topup_amount,
    if(isNull(topup_order.total_topup_number), 0, topup_order.total_topup_number) AS total_topup_number
FROM
(
    SELECT
        user.*,
        if(isNull(user_order.user_id), 0, user_order.total_amount) AS total_order_amount,
        if(isNull(user_order.user_id), 0, user_order.total_order_number) AS total_order_number
    FROM dwd.dwd_Bethune_user_detail AS user
    LEFT JOIN
    (
        SELECT
            user_id,
            sum(if(isNull(amount), 0, amount)) AS total_amount,
            count(*) AS total_order_number
        FROM dwd.dwd_Bethune_order_detail AS dbo
        WHERE (dbo.type = '2') AND (status NOT IN ('0', '2', '3', '5'))
        GROUP BY user_id
    ) AS user_order ON user.user_id = user_order.user_id
) AS t1
LEFT JOIN
(
    SELECT
        user_id,
        sum(if(isNull(amount), 0, amount)) AS total_topup_amount,
        count(*) AS total_topup_number
    FROM dwd.dwd_Bethune_top_up_order_detail
    WHERE pay_status = 'PAID'
    GROUP BY user_id
) AS topup_order ON t1.user_id = topup_order.user_id;

drop table if exists dws.dws_Bethune_user;

rename table dws.dws_Bethune_user_tmp to dws.dws_Bethune_user;
"
