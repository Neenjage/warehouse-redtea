#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password '' --multiquery --multiline -q"
drop table if exists dwd.dwd_Nobel_orders_detail_tmp;

create table dwd.dwd_Nobel_orders_detail_tmp
Engine=MergeTree
order by order_id as
select
t6.*,
payment.name as payment_method_name
from
(select
t5.*,
currency.CNY_rate as currency_CNY_rate,
currency.name as currency_name,
currency.remark as currency_remark,
if(t5.order_price/10000/currency.CNY_rate=inf,0,t5.order_price/10000/currency.CNY_rate) as order_CNYamount
from
(select
t4.*,
ip_address.ip,
ip_address.address,
ip_address.country,
ip_address.province,
ip_address.city
from
(select
t3.*,
user_device.model,
user_device.app_version
from
(select
t1.*,
user.id as user_id
from
(SELECT
  dpo.order_id as order_id,
  'Nobel' as source,
  0 as agent_id,
  'redtea_go' as agent_name,
  dpo.cid,
  dpo.iccid,
  dpo.start_time,
  dpo.end_time,
  dpo.order_price as data_plan_order_price,
  topup_order.order_price as topup_order_price,
  (dpo.order_price + topup_order.order_price) as order_price,
  topup_order.topup_order_count,
  topup_order.topup_order_count + 1 as total_orders,
  dpo.create_time,
  dpo.last_update_time,
  dpo.email_box as email,
  dpo.resource_status,
  dpo.resource_id,
  dpo.location_name,
  dpo.qr_resource_id,
  dpo.source_type,
  dpo.area_id,
  dpo.data_plan_volume_id,
  dpo.data_plan_day_id,
  dpo.qr_iccid,
  dpo.payment_methods_id as payment_method_id,
  dpo.currency_id,
  dpo.status as pay_status,
  dpo.order_status,
  dpo.day_client_resource_id,
  dpo.qr_imsi as imsi,
  dpo.qr_transaction_id as transaction_code,
  dpo.device_id,
  dpo.user_id,
  dpo.effective_time,
  dpo.invalid_time
from
ods.ods_Nobel_data_plan_order dpo
left join
(select
  dpo_order_no,
  sum(order_price) as order_price,
  count(*) as topup_order_count
from
ods.ods_Nobel_data_plan_topup_order
where top_up_status = 'SUCCESS'
and invalid_time = '2105-12-31 23:59:59'
group by dpo_order_no) topup_order
on dpo.order_id = topup_order.dpo_order_no) t1
left join
(select
* from
ods.ods_Nobel_users
where status = 'ACTIVE'
and invalid_time = '2105-12-31 23:59:59') user
on t1.email = user.email) t3
left join
(select
    user_id,
    max(model) as model,
    max(app_version) as app_version
from
ods.ods_Nobel_user_device
group by user_id) user_device
on t3.user_id = user_device.user_id) t4
left join
ods.ods_Nobel_order_ip_address ip_address
on t4.order_id = ip_address.order_id) t5
LEFT JOIN
(
    SELECT
      currency_name.id,
      currency_name.name,
      currency_name.remark,
      currency_rate.CNY_rate
    FROM
        (SELECT
            id,
            name,
            remark
        FROM dim.dim_Nobel_currency
        WHERE import_time = '$import_time') currency_name
    left join
        (select
          name,
          CNY_rate
        FROM dim.dim_Bumblebee_currency_rate
        WHERE import_time = '$import_time') currency_rate
    ON currency_name.name = currency_rate.name
) AS currency ON t5.currency_id = currency.id) t6
left join
(select
*
from dim.dim_Nobel_payment_methods
 where import_time = '$import_time')payment
on t6.payment_method_id = payment.id;

drop table if exists dwd.dwd_Nobel_orders_detail;

rename table dwd.dwd_Nobel_orders_detail_tmp to dwd.dwd_Nobel_orders_detail;
"
