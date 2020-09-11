#!/bin/bash

source  /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi


clickhouse-client -u$user --multiquery -q"
CREATE TABLE ods.ods_Nobel_data_plan_topup_order_temp
ENGINE = MergeTree
ORDER BY id AS
SELECT
    id,
    dpo_order_no,
    dpo_resource_id,
    dpo_qr_resource_id,
    data_plan_type,
    data_volume,
    day_count,
    order_price,
    discount_price,
    present_price,
    order_status,
    status,
    update_time,
    create_time,
    currency_id,
    user_id,
    order_no,
    top_up_Id,
    bell_data_volume,
    bell_top_up_code,
    source_type,
    top_up_status,
    out_top_up_no,
    effective_time,
    if(b.id = 0, a.invalid_time, b.update_time) AS invalid_time
FROM ods.ods_Nobel_data_plan_topup_order AS a
ANY LEFT JOIN
(
    SELECT
        id,
        update_time
    FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'data_plan_topup_order', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
    WHERE update_time >
    (
        SELECT max(update_time)
        FROM ods.ods_Nobel_data_plan_topup_order
    )
) AS b USING (id)
"

clickhouse-client -u$user --multiquery -q"DROP TABLE ods.ods_Nobel_data_plan_topup_order"


clickhouse-client -u$user --multiquery -q"RENAME TABLE ods.ods_Nobel_data_plan_topup_order_temp TO ods.ods_Nobel_data_plan_topup_order"


clickhouse-client -u$user --multiquery -q"
INSERT INTO ods.ods_Nobel_data_plan_topup_order
SELECT
      id,
      dpo_order_no,
      dpo_resource_id,
      dpo_qr_resource_id,
      data_plan_type,
      data_volume,
      day_count,
      order_price,
      discount_price,
      present_price,
      order_status,
      status,
      update_time,
      create_time,
      currency_id,
      user_id,
      order_no,
      top_up_Id,
      bell_data_volume,
      bell_top_up_code,
      source_type,
      top_up_status,
      out_top_up_no,
      update_time as effective_time,
      toDateTime('2105-12-31 23:59:59') AS invalid_time
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'data_plan_topup_order', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
WHERE update_time >
(
    SELECT max(update_time)
    FROM ods.ods_Nobel_data_plan_topup_order
)
"