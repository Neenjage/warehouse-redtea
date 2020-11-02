#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password '' --multiquery --multiline -q"
drop table if exists dws.dws_Nobel_order_tmp;

create table dws.dws_Nobel_order_tmp
ENGINE=MergeTree
order by new_user_order_flag as
select
  toString(order.order_id) as order_id,
  order.email,
  order.create_time,
  order.last_update_time as update_time,
  order.order_price,
  order.source_type,
  user.register_time,
  order.pay_status,
  if(toDate(order.create_time) = toDate(user.register_time),1,0) as new_user_order_flag
from
dwd.dwd_Nobel_orders_detail as order
left join
dwd.dwd_Nobel_users_detail as user on order.email = user.email
where order.invalid_time = '2105-12-31 23:59:59';

drop table if exists dws.dws_Nobel_order_tmp1;

create table dws.dws_Nobel_order_tmp1
ENGINE=MergeTree
order by new_user_order_flag as
select
  'order' as product_type,
  order_id,
  email,
  create_time,
  update_time,
  order_price,
  toString(source_type) as source_type,
  register_time,
  toString(pay_status) as status,
  new_user_order_flag
from
dws.dws_Nobel_order_tmp
union all
select
  'topup_order' as product_type,
  toString(topup_order.dpo_order_no) as order_id,
  order.email,
  topup_order.create_time,
  topup_order.update_time,
  topup_order.order_price,
  topup_order.source_type,
  order.register_time,
  toString(topup_order.top_up_status) as status,
  if(toDate(topup_order.create_time) = toDate(order.register_time),1,0) as new_user_order_flag
from
dwd.dwd_Nobel_topup_orders_detail as  topup_order
left join
(select
  order_id,
  email,
  register_time
from
dws.dws_Nobel_order_tmp
) as order
on topup_order.dpo_order_no = order.order_id;

drop table if exists dws.dws_Nobel_order;

rename table dws.dws_Nobel_order_tmp1 to dws.dws_Nobel_order;

drop table if exists dws.dws_Nobel_order_tmp;

create table dws.dws_Nobel_order_tmp
ENGINE=MergeTree
order by product_type as
select
order.*,
if(first_order.email is null,0,1) as user_first_order_flag
from
dws.dws_Nobel_order as order
left join
(select
  email,
  product_type,
  min(create_time) as first_order_time
from
dws.dws_Nobel_order
where status = '1'
and order_price >= 10000
and create_time > '2020-02-22 23:59:59'
and product_type = 'order'
group by email,product_type
union all
select
  email,
  product_type,
  min(create_time) as first_order_time
from
dws.dws_Nobel_order
where status = 'SUCCESS'
and create_time > '2020-02-22 23:59:59'
and product_type = 'topup_order'
group by email,product_type
) first_order
on order.email = first_order.email and order.create_time = first_order.first_order_time and order.product_type = first_order.product_type;

drop table if exists dws.dws_Nobel_order;

rename table dws.dws_Nobel_order_tmp to dws.dws_Nobel_order;
"