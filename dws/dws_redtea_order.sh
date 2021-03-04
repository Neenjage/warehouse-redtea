#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

#将话单表中transaction_id为0，已转换为-1的数据纳入成本中(没有匹配到相关订单，但有流量消耗)

#data_plan_volume = -1 按天不限量
#data_plan_vomue = 1024000不限量

#成本逻辑业务
#bundle_price有值表示为预付费，只要使用该bundle，并且激活就是bundle_price的成本
#carrier_id = 218 表示这个该运营商在2020年9月让我们帮忙消耗流量，所以成本为0，MTX土耳其免费流量
#订单状态为OBSOLETE的为过期都未激活，所以流量成本为0。
#data_plan_type = 99 表示免费流量。没买套餐免费送20-30兆(能匹配到话单，但是一个话单会对应多个订单，所以以订单角度即默认用户使用完我们送的套餐流量计算成本)。

#以下为国内的相关的流量单价收费情况  如果order_volume 表中有相关订单的流量上传以该流量为消耗计算成本，如果没有则以套餐表中的流量计算成本
#carrier	bundle—Group	name	                      单卡成本	每GB	    流量计算方式
#105	      -	      联通冰激凌卡-39元20G	              25	  1.25	    包月，每月20GB高速，不限量低速
#209	      -   	  联通冰激凌卡-129元30G-API控制关停	    60	    2	      包月，单卡30GB 超过流量计算为3块每GB
#212	      -	      联通冰激凌卡-239元80G-API控制关停	    160	    2	      包月，单卡80GB
#214	    268	      佛山移动-手机-20GB	                49	    2.45	  包月，单卡20GB
#214	    252	      佛山移动-手机-50GB	                105	    2.1	    包月，单卡50GB
#216	      -	      IOE联通-手机	                      19.5	  1.3	    包月，流量池=激活卡数量*15GB
#219	      -	      深圳移动-手机	                      12	    2	      包月，流量池=激活卡数量*6GB
#223	      -	      连连科技	                          -	1	      每日1元800MB


#国外话单没匹配到成本的情况下
#如果没匹配到话单表，但是落地运营商单价，消耗流量知道，cdr.total_usage/1024/1024/1024*total2.local_carrier_price
#如果没匹配到话单表，落地运营单价也不知道，采用套餐的流量大小(默认套餐全部使用完，成本会偏高)， total2.data_plan_volume/1024*total2.local_carrier_price


#order_status = 'OBSOLETE' and cost != 0  AND total_usage = 0 表示该订单为预付费订单套餐，即bundle_price不为空


clickhouse-client --user $user --password $password --multiquery --multiline --max_memory_usage 30000000000 -q"
drop table if exists dws.dws_redtea_order_tmp;

CREATE TABLE dws.dws_redtea_order_tmp
ENGINE = MergeTree
ORDER BY order_id AS
SELECT
  total3.*,
  pay_account.account
FROM
(SELECT
    total2.*,
    cdr.total_usage AS total_usage,
    if(total2.order_status = 'OBSOLETE' and cdr.total_usage is null,0,
      if(isNotNull(total2.bundle_price) and total2.bundle_price != 0,total2.bundle_price,
      if(carrier_id = 218,0,
      if(total2.data_plan_type = 99 and cdr.total_usage is null,0,
      if(total2.data_plan_type = 99 and local_carrier_price != 0 and data_plan_volume > 0,total2.data_plan_volume/1024*total2.local_carrier_price,
      if(cdr.cost != 0,cdr.cost,
      if(total2.carrier_id = 223 ,if(total2.volume_usage is not null,total2.volume_usage/1024/1024/800,total2.data_plan_volume/800),
      if(total2.carrier_id = 219 ,if(total2.volume_usage is not null,total2.volume_usage/1024/1024/1024*2,total2.data_plan_volume/1024*2),
      if(total2.carrier_id = 216 ,if(total2.volume_usage is not null,total2.volume_usage/1024/1024/1024*1.3,total2.data_plan_volume/1024*1.3),
      if(total2.carrier_id = 214 and total2.bundle_group_id = 252 ,if(total2.volume_usage is not null,total2.volume_usage/1024/1024/1024*2.1,total2.data_plan_volume/1024*2.1),
      if(total2.carrier_id = 214 and total2.bundle_group_id = 268 ,if(total2.volume_usage is not null,total2.volume_usage/1024/1024/1024*2.45,total2.data_plan_volume/1024*2.45),
      if(total2.carrier_id = 212 ,if(total2.volume_usage is not null,total2.volume_usage/1024/1024/1024*2,total2.data_plan_volume/1024*2),
      if(total2.carrier_id = 209 ,if(total2.source='Einstein',if(total2.volume_usage is not null,total2.volume_usage/1024/1024/1024*2,total2.data_plan_volume/1024*2),
                                    if(total2.volume_usage is not null,
                                        if(total2.volume_usage/1024/1024/1024<=30, total2.volume_usage/1024/1024/1024*2, (total2.volume_usage/1024/1024/1024-30)*3+60),
                                        if(data_plan_volume = 1024000,60,data_plan_volume/1024*2))),
      if(total2.carrier_id = 105,if(total2.source='Einstein',
            if(total2.volume_usage is not null,total2.volume_usage/1024/1024/1024*1.25,total2.data_plan_volume/1024*1.25),
            if(total2.volume_usage is not null and total2.volume_usage/1024/1024/1024 <= 20,total2.volume_usage/1024/1024/1024*1.25,
            if(data_plan_volume = 1024000,25,total2.data_plan_volume/1024*1.25))),
      if(total2.local_carrier_price is not null and total2.local_carrier_price !=0 and cdr.total_usage != 0,cdr.total_usage/1024/1024/1024*total2.local_carrier_price,
      if(total2.local_carrier_price is not null and total2.data_plan_volume >0,total2.data_plan_volume/1024*total2.local_carrier_price,
      if(total2.activate_time is NOT NULL and total2.volume_usage is NOT NULL and total2.local_carrier_price != 0 and total2.local_carrier_price is not null,total2.volume_usage/1024/1024/1024 * total2.local_carrier_price,0))))))))))))))))) AS total_cost,
    if(isNull(((total2.order_CNYamount - total2.transation_fee) - total2.revenue_share) - total_cost), 0, ((total2.order_CNYamount - total2.transation_fee) - total2.revenue_share) - total_cost) AS net_amount,
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
        bundle_detail.bundle_group_name,
        bundle_detail.local_carrier_id,
        bundle_detail.local_carrier_name,
        bundle_detail.location_code,
        bundle_detail.local_carrier_price
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
                    t2.location_id,
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
                    device.model as model,
                    t2.order_status,
                    t2.activate_time,
                    t2.expiration_time,
                    t2.end_time,
                    t2.order_time,
                    t2.update_time,
                    t2.purchased_ip,
                    t2.order_address,
                    t2.payment_time,
                    t2.refund_time,
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
                            case when data_plan_detail.data_plan_name like '%流量宝%' then 'Bethune'
                                 else order_detail.order_source end AS source,
                            toInt8(order_detail.agent_id) AS agent_id,
                            order_detail.agent_name,
                            order_detail.data_plan_id,
                            data_plan_detail.data_plan_name,
                            data_plan_detail.data_plan_type,
                            data_plan_detail.data_plan_volume,
                            data_plan_detail.location_id,
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
                            order_detail.payment_time,
                            order_detail.refund_time,
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
                                location_id,
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
                        order_detail.area_id as location_id,
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
                        order_detail.model AS model,
                        order_detail.order_status,
                        order_detail.start_time AS activate_time,
                        order_detail.end_time AS expiration_time,
                        order_detail.end_time AS end_time,
                        order_detail.create_time AS order_time,
                        order_detail.last_update_time AS update_time,
                        order_detail.ip AS purchased_ip,
                        'unknown' AS order_address,
                        order_detail.payment_time,
                        null as refund_time,
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
            bundle.bundle_code,
            bundle.bundle_price,
            bundle.bundle_id,
            bundle.bundle_name,
            bundle.carrier_id,
            bundle.carrier_name,
            bundle.bundle_group_id,
            bundle.bundle_group_name,
            local_carrier.local_carrier_id,
            local_carrier.local_carrier_name,
            local_carrier.location_code,
            if(bundle.local_carrier_price is not null,bundle.local_carrier_price,local_carrier.local_carrier_price) as local_carrier_price
        FROM dwd.dwd_Bumblebee_bundle_detail bundle
        LEFT JOIN dwd.dwd_Bumblebee_local_carrier_detail local_carrier
        ON bundle.carrier_id = local_carrier.carrier_id and bundle.local_carrier_plmns = local_carrier.location_code and bundle.bundle_group_id = local_carrier.bundle_group_id
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
) AS cdr ON total2.transaction_id = cdr.transaction_id) as total3
left join dwd.dwd_Einstein_order_pay_account_detail pay_account
on total3.order_id = toString(pay_account.order_id) and toString(total3.payment_method_id) = toString(pay_account.pay_method_id);

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
and order_status not in ('REFUNDED','REFUNDING','RESERVED')
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
total_cost,
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