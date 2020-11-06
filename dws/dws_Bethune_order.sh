#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dws.dws_Bethune_order_tmp;

create table dws.dws_Bethune_order_tmp
Engine=MergeTree
order by id as
select
	order.id,
	order.source,
	order.user_id,
	order.data_plan_id,
	order.order_imei,
	order.order_device,
	order.order_no,
	order.create_time,
	order.count,
	order.amount,
	order.type,
	order.status,
	order.Einstein_order_id,
	order.payment_method,
	order.order_model,
	order.order_brand,
	order.user_ip,
	user.user_status,
	user.create_time as register_time,
	user.login_time,
	user.recommend_user,
	user.imei as user_imei,
	user.device_id as user_device,
	user.brand as user_brand,
	user.model as user_model,
	if(toDate(addHours(order.create_time,8)) = toDate(addHours(user.create_time,8)),1,0) as new_user_order_flag
from
(select
	order.id,
	'order' as source,
	order.user_id,
	toString(order.data_plan_id) as data_plan_id,
	order.imei as order_imei,
	order.device_id as order_device,
	order.order_no,
	order.create_time,
	order.count,
	order.amount,
	toString(order.type) as type,
	toString(order.status) as status,
	order.Einstein_order_id,
  order.payment_method,
	order.model as order_model,
	order.brand as order_brand,
	order.user_ip
from dwd.dwd_Bethune_order_detail as order
union all
select
top_up_order.id,
'top_order' as source,
top_up_order.user_id,
top_up_order.product_name as data_plan_id,
'unknown' as order_imei,
'unknown' as order_device,
'unknown' as order_no,
top_up_order.create_time,
1 as count,
top_up_order.amount,
top_up_order.top_up_type as type,
top_up_order.pay_status as status,
'unknown' as Einstein_order_id,
payment_mode as payment_method,
'unknown' as order_model,
'unknown' as order_brand,
'unknown' as user_ip
from
dwd.dwd_Bethune_top_up_order_detail top_up_order) as order
left join dwd.dwd_Bethune_user_detail as user
on order.user_id = user.user_id;

drop table if exists dws.dws_Bethune_order;

rename table dws.dws_Bethune_order_tmp to dws.dws_Bethune_order;

create table dws.dws_Bethune_order_tmp
Engine=MergeTree
order by id as
SELECT
do.*,
if(min.user_id is null,0,1) as user_first_order_flag
FROM
dws.dws_Bethune_order do
left join
(SELECT
order.user_id,
min(create_time) as first_create_time
FROM
(SELECT
order_no,
user_id,
amount,
create_time,
register_time
FROM dws.dws_Bethune_order dbo
where dbo.source = 'order'
and dbo.type = '2'
and status not IN ('0','2','3','5')) as order
GROUP by order.user_id) as min
on do.user_id = min.user_id and do.create_time = min.first_create_time;

drop table if exists dws.dws_Bethune_order;

rename table dws.dws_Bethune_order_tmp to dws.dws_Bethune_order;
"

