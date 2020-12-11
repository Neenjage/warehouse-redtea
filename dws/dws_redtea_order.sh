#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

#将话单表中transaction_id为0，已转换为-1的数据纳入成本中(没有匹配到相关订单，但有流量消耗)
clickhouse-client --user $user --password $password --multiquery --multiline --max_memory_usage 30000000000 -q"
drop table if exists dws.dws_redtea_order_tmp;

CREATE TABLE dws.dws_redtea_order_tmp
ENGINE = MergeTree
ORDER BY order_id AS
SELECT
    total2.*,
    if(isNull(cdr.total_usage), 0, cdr.total_usage) AS total_usage,
    if(isNotNull(total2.bundle_price),total2.bundle_price,if(isNotNull(cdr.cost),cdr.cost,0)) AS cost,
    if(isNull(((total2.order_CNYamount - total2.transation_fee) - total2.revenue_share) - if(isNotNull(total2.bundle_price),total2.bundle_price,if(isNotNull(cdr.cost),cdr.cost,0))), 0, ((total2.order_CNYamount - total2.transation_fee) - total2.revenue_share) - if(isNotNull(total2.bundle_price),total2.bundle_price,if(isNotNull(cdr.cost),cdr.cost,0))) AS net_amount,
    if(toDate(addHours(order_time, 8)) = toDate(addHours(register_time, 8)), 1, 0) AS new_user_order_flag
FROM
(
    SELECT
        total1.*,
        bundle_detail.bundle_id,
        bundle_detail.bundle_name,
        bundle_detail.bundle_price,
        bundle_detail.carrier_id,
        bundle_detail.carrier_name,
        bundle_detail.bundle_group_id,
        bundle_detail.bundle_group_name
    FROM
    (
        SELECT
            total.*,
            if(transaction.transaction_id = 0, -2, transaction.transaction_id) AS transaction_id
        FROM
        (
            SELECT
                t3.*,
                pay_detail.buyer_id
            FROM
            (
                SELECT
                    t2.order_id,
                    t2.order_no,
                    t2.source,
                    t2.agent_id AS agent_id,
                    t2.agent_name,
                    t2.data_plan_id,
                    t2.data_plan_name,
                    t2.data_plan_type,
                    t2.data_plan_volume,
                    t2.order_location_name,
                    t2.volume_usage,
                    t2.imsi,
                    t2.order_CNYamount,
                    t2.payment_method_id,
                    t2.payment_method_name,
                    t2.transation_fee,
                    t2.revenue_share,
                    t2.currency_id,
                    t2.currency_name,
                    t2.currency_CNY_rate,
                    t2.user_id,
                    t2.email,
                    t2.device_id AS device_id,
                    device.brand AS brand,
                    t2.order_status,
                    t2.activate_time,
                    t2.expiration_time,
                    t2.end_time,
                    t2.order_time,
                    t2.update_time,
                    t2.purchased_ip,
                    t2.order_address,
                    t2.effective_time,
                    t2.invalid_time,
                    t2.transaction_code,
                    t2.bundle_code,
                    device.residence,
                    device.register_time,
                    device.uid_level
                FROM dwd.dwd_Einstein_device_detail AS device
                INNER JOIN
                (
                    SELECT
                        t1.*,
                        oipr.transaction_code,
                        oipr.bundle_code
                    FROM
                    (
                        SELECT
                            toString(order_detail.order_id) AS order_id,
                            order_detail.order_no,
                            order_detail.order_source AS source,
                            toInt8(order_detail.agent_id) AS agent_id,
                            order_detail.agent_name,
                            order_detail.data_plan_id,
                            data_plan_detail.data_plan_name,
                            data_plan_detail.data_plan_type,
                            data_plan_detail.data_plan_volume,
                            data_plan_detail.location_remark AS order_location_name,
                            order_detail.volume_usage,
                            order_detail.imsi,
                            order_detail.order_CNYamount,
                            order_detail.payment_method_id,
                            order_detail.payment_method_name,
                            if(order_detail.payment_method_id = 4, (order_detail.order_CNYamount * 6) / 1000, if(order_detail.payment_method_id = 9, (order_detail.order_CNYamount * 8) / 1000, (order_detail.order_CNYamount * 4) / 100)) AS transation_fee,
                            if((order_detail.agent_id = 1) OR (order_detail.agent_id = 14), if(startsWith(order_detail.imsi, '460'), order_detail.order_CNYamount * 0.3, order_detail.order_CNYamount * 0.18), if((order_detail.agent_id = 9) AND startsWith(order_detail.imsi, '460'), order_detail.order_CNYamount * 0.15, order_detail.order_CNYamount * 0.1)) AS revenue_share,
                            order_detail.currency_id,
                            order_detail.currency_name,
                            order_detail.currency_CNY_rate,
                            toInt32(-1) AS user_id,
                            'Einstein' AS email,
                            order_detail.device_id,
                            order_detail.order_status,
                            order_detail.activate_time,
                            order_detail.expiration_time,
                            order_detail.end_time,
                            order_detail.order_time,
                            order_detail.update_time,
                            order_detail.order_ip AS purchased_ip,
                            order_detail.order_address,
                            order_detail.effective_time,
                            order_detail.invalid_time
                        FROM dwd.dwd_Einstein_orders_detail AS order_detail
                        LEFT JOIN
                        (
                            SELECT
                                data_plan_id,
                                data_plan_name,
                                data_plan_type,
                                data_plan_volume,
                                location_remark
                            FROM dwd.dwd_Einstein_data_plan_detail
                        ) AS data_plan_detail ON order_detail.data_plan_id = toString(data_plan_detail.data_plan_id)
                    ) AS t1
                    LEFT JOIN dwd.dwd_Einstein_order_imsi_profile_relation AS oipr ON t1.order_id = toString(oipr.order_id)
                ) AS t2 ON t2.device_id = device.device_id
            ) AS t3
            LEFT JOIN dwd.dwd_Einstein_pay_detail AS pay_detail ON t3.order_no = pay_detail.out_trade_no
            UNION ALL
            SELECT
                t2.*,
                users.register_time,
                -10000 AS uid_level,
                'unknown' AS buyer_id
            FROM
            (
                SELECT
                    t1.*,
                    resource_detail.bundle_code,
                    'Not_cn' AS residence
                FROM
                (
                    SELECT
                        order_detail.order_id,
                        '0' AS order_no,
                        order_detail.source,
                        toInt8(order_detail.agent_id) AS agent_id,
                        order_detail.agent_name,
                        toString(order_detail.day_client_resource_id) AS data_plan_id,
                        toString(data_plan_detail.data_plan_volume) AS data_plan_name,
                        0 AS data_plan_type,
                        data_plan_detail.data_plan_volume,
                        order_detail.location_name AS order_location_name,
                        0 AS volume_usage,
                        order_detail.imsi,
                        order_detail.order_CNYamount,
                        order_detail.payment_method_id,
                        order_detail.payment_method_name,
                        toFloat64(0) AS transation_fee,
                        toFloat64(0) AS revenue_share,
                        order_detail.currency_id,
                        order_detail.currency_name,
                        order_detail.currency_CNY_rate,
                        order_detail.user_id,
                        order_detail.email,
                        order_detail.device_id,
                        order_detail.model AS brand,
                        order_detail.order_status,
                        order_detail.start_time AS activate_time,
                        order_detail.end_time AS expiration_time,
                        order_detail.end_time AS end_time,
                        order_detail.create_time AS order_time,
                        order_detail.last_update_time AS update_time,
                        order_detail.ip AS purchased_ip,
                        'unknown' AS order_address,
                        order_detail.effective_time,
                        order_detail.invalid_time,
                        order_detail.transaction_code
                    FROM dwd.dwd_Nobel_orders_detail AS order_detail
                    LEFT JOIN
                    (
                        SELECT
                            day_client_resource_id,
                            data_plan_volume
                        FROM dwd.dwd_Nobel_data_plan_detail
                    ) AS data_plan_detail ON order_detail.day_client_resource_id = data_plan_detail.day_client_resource_id
                ) AS t1
                LEFT JOIN
                (
                    SELECT
                        transaction_code,
                        bundle_code
                    FROM dwd.dwd_Bell_imsi_resource_detail
                    WHERE isNotNull(transaction_code) AND (transaction_code != '')
                ) AS resource_detail ON t1.transaction_code = resource_detail.transaction_code
            ) AS t2
            LEFT JOIN dwd.dwd_Nobel_users_detail AS users ON t2.email = users.email
        ) AS total
        LEFT JOIN dwd.dwd_Bumblebee_imsi_transaction_detail AS transaction ON total.transaction_code = transaction.transaction_code
    ) AS total1
    LEFT JOIN
    (
        SELECT
            bundle_code,
            bundle_price,
            bundle_id,
            bundle_name,
            carrier_id,
            carrier_name,
            bundle_group_id,
            bundle_group_name
        FROM dwd.dwd_Bumblebee_bundle_detail
    ) AS bundle_detail ON total1.bundle_code = bundle_detail.bundle_code
) AS total2
LEFT JOIN
(
    SELECT
        cdr_raw.transaction_id,
        sum(cdr_raw.total_usage) AS total_usage,
        sum(cdr_raw.cost) AS cost
    FROM dwd.dwd_Bumblebee_imsi_transaction_cdr_raw AS cdr_raw
    WHERE cdr_raw.transaction_id != -1
    GROUP BY cdr_raw.transaction_id
) AS cdr ON total2.transaction_id = cdr.transaction_id;

drop table if exists dws.dws_redtea_order;

rename table dws.dws_redtea_order_tmp to dws.dws_redtea_order;

drop table if exists dws.dws_redtea_order_tmp1;

create table dws.dws_redtea_order_tmp1
Engine=MergeTree
order by order_id as
select
order.* ,
if(first_order.device_id is null,0,1) as user_first_order_flag
from
dws.dws_redtea_order as order
left join
(select
  device_id,
  min(order_time) as first_order_time
from
dws.dws_redtea_order
where source = 'Einstein'
and order_status not in ('REFUNDED','REFUNDING33','RESERVED')
and order_CNYamount > 0
and invalid_time = '2105-12-31 23:59:59'
group by device_id) first_order
on order.device_id = first_order.device_id and order.order_time = first_order.first_order_time;

drop table if exists dws.dws_redtea_order;

rename table dws.dws_redtea_order_tmp1 to dws.dws_redtea_order;

alter table dws.dws_redtea_order delete where transaction_id = -1;

INSERT INTO TABLE dws.dws_redtea_order(
order_id,
currency_name,
source,
transaction_id,
bundle_id,
carrier_id,
bundle_group_id,
total_usage,
cost,
transation_fee,
revenue_share,
net_amount)
select
  -1,
  'unknown',
  'unknown',
  cdr_raw.transaction_id,
  -1,
  -1,
  -1,
  sum(cdr_raw.total_usage) as total_usage,
  sum(cdr_raw.cost) as cost,
  0,
  0,
  -sum(cdr_raw.cost)
from
dwd.dwd_Bumblebee_imsi_transaction_cdr_raw cdr_raw
where cdr_raw.transaction_id = -1
group by
  cdr_raw.transaction_id;
"