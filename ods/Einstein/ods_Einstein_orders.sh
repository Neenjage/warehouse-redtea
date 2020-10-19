#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

# 将被更改的老数据设置为过期数据,目的保存老数据，拉链表形式(有新增，有更改)
# 查询历史数据 采用 "invalid小于参数时间,并且update_time 小于参数时间"  "或者invalid_time = '2105-12-31 23:59:59' 并且update_time 小于参数时间"
clickhouse-client --user $user --password '' --multiquery --multiline -q"
create table if not exists ods.ods_Einstein_orders
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

drop table if exists ods.ods_Einstein_orders_temp;

CREATE TABLE ods.ods_Einstein_orders_temp
ENGINE = MergeTree
ORDER BY id AS
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
    effective_time,
    if(b.id = 0, a.invalid_time, b.update_time) AS invalid_time
FROM ods.ods_Einstein_orders AS a
ANY LEFT JOIN
(
    SELECT
        id,
        update_time
    FROM mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'orders', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c')
    WHERE update_time >
    (
        SELECT max(update_time)
        FROM ods.ods_Einstein_orders
    )
) AS b USING (id);

DROP TABLE IF EXISTS ods.ods_Einstein_orders;

RENAME TABLE ods.ods_Einstein_orders_temp TO ods.ods_Einstein_orders;

INSERT INTO ods.ods_Einstein_orders
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
FROM mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'orders', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c')
WHERE update_time >
(
    SELECT max(update_time)
    FROM ods.ods_Einstein_orders
);
"




