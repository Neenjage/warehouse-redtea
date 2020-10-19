#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password '' --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS ods.ods_Nobel_data_plan_order
ENGINE=MergeTree
order by id as
SELECT
    id,
    order_id,
    cid,
    iccid,
    out_data_plan_id,
    out_order_id,
    start_time,
    day_count,
    end_time,
    data_plan_id,
    data_plan_timezone_fix,
    data_plan_price,
    order_price,
    status,
    create_time,
    last_update_time,
    data_plan_type,
    delivery_status,
    email_box,
    qrcode_url,
    data_plan_name,
    resource_status,
    delivery_time,
    resource_id,
    data_volume,
    location_name,
    is_hide,
    refund_reason,
    qr_resource_id,
    gain_resource_time,
    source_type,
    imei,
    data_plan_volume,
    data_plan_day,
    area_id,
    is_deleted,
    qr_activation_code,
    data_plan_volume_id,
    data_plan_day_id,
    qr_confirm_code,
    qr_iccid,
    payment_methods_id,
    refund_time,
    currency_id,
    order_status,
    support_cdr,
    valid_day,
    day_client_resource_id,
    qr_imsi,
    qr_transaction_id,
    device_id,
    volume_alert_push_times,
    user_id,
    last_update_time AS effective_time,
    toDateTime('2105-12-31 23:59:59') AS invalid_time
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'data_plan_order', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13');

DROP TABLE IF EXISTS ods.ods_Nobel_data_plan_order_tmp;

CREATE TABLE ods.ods_Nobel_data_plan_order_tmp
ENGINE = MergeTree
ORDER BY id AS
SELECT
    id,
    order_id,
    cid,
    iccid,
    out_data_plan_id,
    out_order_id,
    start_time,
    day_count,
    end_time,
    data_plan_id,
    data_plan_timezone_fix,
    data_plan_price,
    order_price,
    status,
    create_time,
    last_update_time,
    data_plan_type,
    delivery_status,
    email_box,
    qrcode_url,
    data_plan_name,
    resource_status,
    delivery_time,
    resource_id,
    data_volume,
    location_name,
    is_hide,
    refund_reason,
    qr_resource_id,
    gain_resource_time,
    source_type,
    imei,
    data_plan_volume,
    data_plan_day,
    area_id,
    is_deleted,
    qr_activation_code,
    data_plan_volume_id,
    data_plan_day_id,
    qr_confirm_code,
    qr_iccid,
    payment_methods_id,
    refund_time,
    currency_id,
    order_status,
    support_cdr,
    valid_day,
    day_client_resource_id,
    qr_imsi,
    qr_transaction_id,
    device_id,
    volume_alert_push_times,
    user_id,
    effective_time,
    if(b.id = 0, a.invalid_time, b.last_update_time) AS invalid_time
FROM ods.ods_Nobel_data_plan_order AS a
ANY LEFT JOIN
(
    SELECT
        id,
        last_update_time
    FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'data_plan_order', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
    WHERE last_update_time >
    (
        SELECT max(last_update_time)
        FROM ods.ods_Nobel_data_plan_order
    )
) AS b USING (id);

DROP TABLE IF EXISTS ods.ods_Nobel_data_plan_order;

RENAME TABLE ods.ods_Nobel_data_plan_order_tmp TO ods.ods_Nobel_data_plan_order;

INSERT INTO ods.ods_Nobel_data_plan_order
SELECT
    id,
    order_id,
    cid,
    iccid,
    out_data_plan_id,
    out_order_id,
    start_time,
    day_count,
    end_time,
    data_plan_id,
    data_plan_timezone_fix,
    data_plan_price,
    order_price,
    status,
    create_time,
    last_update_time,
    data_plan_type,
    delivery_status,
    email_box,
    qrcode_url,
    data_plan_name,
    resource_status,
    delivery_time,
    resource_id,
    data_volume,
    location_name,
    is_hide,
    refund_reason,
    qr_resource_id,
    gain_resource_time,
    source_type,
    imei,
    data_plan_volume,
    data_plan_day,
    area_id,
    is_deleted,
    qr_activation_code,
    data_plan_volume_id,
    data_plan_day_id,
    qr_confirm_code,
    qr_iccid,
    payment_methods_id,
    refund_time,
    currency_id,
    order_status,
    support_cdr,
    valid_day,
    day_client_resource_id,
    qr_imsi,
    qr_transaction_id,
    device_id,
    volume_alert_push_times,
    user_id,
    last_update_time AS effective_time,
    toDateTime('2105-12-31 23:59:59') AS invalid_time
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'data_plan_order', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
WHERE last_update_time >
(
    SELECT max(last_update_time)
    FROM ods.ods_Nobel_data_plan_order
);
"





