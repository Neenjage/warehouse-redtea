#!/bin/bash

source  /home/ops/warehouse-redtea/config/config.sh

import_time=date +%F

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline  --max_memory_usage 30000000000 -q"
CREATE TABLE dws.dws_redtea_user_tmp
Engine=MergeTree
order by user_id as
SELECT
    toString(user.user_id) as user_id,
    user.source,
    user.model as brand,
    user.model,
    user.email,
    user.register_time,
    user.last_login_time,
    if(order.total_order is null,0,order.total_order) as total_orders,
    if(order.total_amount is null,0,order.total_amount) as total_amount,
    if(order.total_cost is null,0,order.total_cost) as total_cost
FROM dwd.dwd_Nobel_users_detail AS user
LEFT JOIN
(
    SELECT
        order_temp2.user_id,
        sum(order_temp2.total_orders) AS total_order,
        sum(if(isNull(order_temp2.order_CNYamount), 0, order_temp2.order_CNYamount)) AS total_amount,
        sum(if(isNull(order_temp2.total_usage), 0, order_temp2.total_usage)) AS total_usage,
        sum(if(isNull(order_temp2.cost), 0, order_temp2.cost)) AS total_cost
    FROM
    (
        SELECT
            order_temp1.user_id,
            order_temp1.total_orders,
            order_temp1.order_CNYamount,
            cdr_raw.total_usage,
            cdr_raw.cost
        FROM
        (
            SELECT
                order_temp.*,
                itd.transaction_id
            FROM
            (
                SELECT
                    user_id,
                    total_orders,
                    order_CNYamount,
                    transaction_code
                FROM dwd.dwd_Nobel_orders_detail
                WHERE (invalid_time = '2105-12-31 23:59:59') AND (pay_status = 1)
            ) AS order_temp
            LEFT JOIN
            (
                SELECT
                    transaction_code,
                    transaction_id
                FROM dwd.dwd_Bumblebee_imsi_transaction_detail
            ) AS itd ON order_temp.transaction_code = itd.transaction_code
        ) AS order_temp1
        LEFT JOIN
        (
            SELECT
                cdr_raw.transaction_id,
                sum(cdr_raw.total_usage) AS total_usage,
                sum(cdr_raw.cost) AS cost
            FROM dwd.dwd_Bumblebee_imsi_transaction_cdr_raw AS cdr_raw
            WHERE cdr_raw.transaction_id != -1
            GROUP BY cdr_raw.transaction_id
        ) AS cdr_raw ON order_temp1.transaction_id = cdr_raw.transaction_id
    ) AS order_temp2
    GROUP BY order_temp2.user_id
) AS order ON user.user_id = order.user_id

union all

select
  device.device_id as user_id,
  'Einstein' as source,
  device.brand,
  device.model,
  'unknown' as email,
  device.register_time,
  device.last_login_time,
  order_detail.order_number as total_orders,
  if(order_detail.toal_amount is null,0,order_detail.toal_amount) as toal_amount,
  if(order_detail.total_cost is null,0,order_detail.total_cost) as total_cost
from
dwd.dwd_Einstein_device_detail device
left join
(select
total.device_id,
count(device_id) as order_number,
sum(if(total.order_CNYamount is null,0,total.order_CNYamount)) as toal_amount,
sum(if(total.cost is null,0,total.cost)) as total_cost
from
(select
  order_transcation.device_id,
  order_transcation.order_CNYamount,
  cdr.cost
from
(select
  order.order_id,
  order.device_id,
  order.order_CNYamount,
  relation.transaction_id
from
(select
  order_id,
  device_id,
  order_CNYamount
from
dwd.dwd_Einstein_orders_detail
where invalid_time = '2105-12-31 23:59:59'
and order_status in ('ACTIVATED','EXPIRED','PURCHASED','OBSOLETE','USEDUP')
and order_amount != 0) as order
left join dwd.dwd_Einstein_order_imsi_profile_relation relation on order.order_id = relation.order_id) as order_transcation
left join dwd.dwd_Bumblebee_imsi_transaction_cdr_raw cdr on order_transcation.transaction_id = cdr.transaction_id
) total
group by total.device_id) as order_detail
on order_detail.device_id = device.device_id

union all

select
  toString(user.user_id) as user_id,
  user.source,
  user.brand,
  user.model,
  user.email,
  user.register_time,
  user.last_login_time,
  if(order.total_orders is null,0,order.total_orders) as total_orders,
  if(order.total_amount is null,0,order.total_amount) as total_amount,
  if(order.total_cost is null,0,order.total_cost) as total_cost
from
(select
  user_id,
  'Bethune' as source,
  brand,
  model,
  'unknown' as email,
  create_time as register_time,
  login_time as last_login_time
from
dwd.dwd_Bethune_user_detail) user
left join
(select
  total.user_id,
  sum(if(amount = 0,0,1)) as total_orders,
  sum(total.amount) as total_amount,
  sum(total.cost) as total_cost
from
(select
  order2.user_id,
  order2.amount,
  if(cdr.cost is null,0,cdr.cost) as cost
from
(select
  order1.user_id,
  order1.amount,
  relation.transaction_id
from
(select
  order.user_id,
  order.amount,
  Einstein_order.order_id
from
(select
  user_id,
  if(status = 2,0,amount/100) as amount,
  Einstein_order_id
from
dwd.dwd_Bethune_orders_detail
where status not in (0,3,5)) as order
left join
(select
 order_id,
 order_no
from
dwd.dwd_Einstein_orders_detail
where invalid_time = '2105-12-31 23:59:59') as Einstein_order
on order.Einstein_order_id = Einstein_order.order_no) as order1
left join dwd.dwd_Einstein_order_imsi_profile_relation relation on order1.order_id = relation.order_id) as order2
left join dwd.dwd_Bumblebee_imsi_transaction_cdr_raw cdr on order2.transaction_id = cdr.transaction_id) as total
group by total.user_id) as order
on user.user_id = order.user_id
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table dws.dws_redtea_user
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
rename table dws.dws_redtea_user_tmp to dws.dws_redtea__user
"










