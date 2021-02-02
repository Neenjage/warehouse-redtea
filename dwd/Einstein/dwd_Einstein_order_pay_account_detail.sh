#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi


#支付方式4与9在dim.dim_Einstein_payment_methods中体现，4为微信，9为支付宝

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dwd.dwd_Einstein_order_pay_account_detail_tmp;

create table dwd.dwd_Einstein_order_pay_account_detail_tmp
ENGINE=MergeTree
order by order_id as
select
order_id,
alipay_account as account,
9 as pay_method_id
from
ods.ods_Einstein_order_alipay_rel order_alipay
left join
ods.ods_Einstein_payment_alipay_config alipay_config
on order_alipay.payment_alipay_config_id = alipay_config.id
union all
select
order_id,
mch_id as account,
4 as pay_method_id
from
ods.ods_Einstein_order_wechat_rel order_wechat
left join
ods.ods_Einstein_payment_wechat_config wechat_config
on order_wechat.payment_wechat_config_id = wechat_config.id;

drop table if exists dwd.dwd_Einstein_order_pay_account_detail;

rename table dwd.dwd_Einstein_order_pay_account_detail_tmp to dwd.dwd_Einstein_order_pay_account_detail;
"