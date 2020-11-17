#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists ods.ods_Einstein_orders_tmp;

create table ods.ods_Einstein_orders_tmp
ENGINE=MergeTree
order by id as
SELECT
    id,
    device_id,
    data_plan_id,
    count,
    order_time,
    status,
    update_time,
    activate_time,
    login_time,
    end_time,
    imsi,
    amount,
    agent_id,
    provider_id,
    provider_order_id,
    payment_method_id,
    order_no,
    expiration_time,
    is_pushed,
    uid,
    is_deleted,
    currency_id,
    balance_pay_amount,
    actual_pay_amount,
    balance_pay_result,
    actual_pay_result,
    refund_reason,
    is_upgrade,
    deal_price,
    bulk_discount,
    channel_id,
    original_sell_price,
    update_time AS effective_time,
    toDateTime('2105-12-31 23:59:59') AS invalid_time
FROM mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'orders', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c');

drop table if exists ods.ods_Einstein_orders;

RENAME TABLE ods.ods_Einstein_orders_tmp TO ods.ods_Einstein_orders;
"
