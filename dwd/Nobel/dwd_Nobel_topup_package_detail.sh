#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password '' --multiquery --multiline -q"
drop table if exists dwd.dwd_Nobel_topup_package_detail_tmp;

create table dwd.dwd_Nobel_topup_package_detail_tmp
Engine=MergeTree
order by id as
select
    id,
    order_no,
    out_order_no ,
    topup_package_id,
    topup_package_name,
    topup_package_price,
    order_price,
    status,
    currency_id ,
    payment_methods_id,
    create_time,
    update_time,
    order_status,
    refund_reason,
    refund_time,
    amount,
    user_id,
    source_type,
    effective_time
FROM ods.ods_Nobel_topup_package_order
where invalid_time = '2105-12-31 23:59:59';

drop table if exists dwd.dwd_Nobel_topup_package_detail;

rename table dwd.dwd_Nobel_topup_package_detail_tmp to dwd.dwd_Nobel_topup_package_detail;
"