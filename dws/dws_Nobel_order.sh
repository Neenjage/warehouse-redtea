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
order by order_id as
select
order.*,
user.register_time,
if(toDate(create_time) = toDate(register_time),1,0) as new_user_order_flag
from
dwd.dwd_Nobel_orders_detail as order
left join
dwd.dwd_Nobel_users_detail as user on order.email = user.email;

drop table if exists dws.dws_Nobel_order;

rename table dws.dws_Nobel_order_tmp to dws.dws_Nobel_order;

drop table if exists dws.dws_Nobel_order_tmp;

create table dws.dws_Nobel_order_tmp
ENGINE=MergeTree
order by order_id as
select
order.*,
if(first_order.email is null,0,1) as user_first_order_flag
from
dws.dws_Nobel_order as order
left join
(select
  email,
  min(create_time) as first_order_time
from
dws.dws_Nobel_order
where pay_status = 1
and data_plan_order_price >= 10000
and create_time > '2020-02-22 23:59:59'
and invalid_time = '2105-12-31 23:59:59'
group by email) first_order
on order.email = first_order.email and order.create_time = first_order.first_order_time;

drop table if exists dws.dws_Nobel_order;

rename table dws.dws_Nobel_order_tmp to dws.dws_Nobel_order;
"