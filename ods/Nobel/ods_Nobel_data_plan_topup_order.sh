#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists ods.ods_Nobel_data_plan_topup_order_tmp;

CREATE TABLE ods.ods_Nobel_data_plan_topup_order_tmp
ENGINE=MergeTree
order by id as
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
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'data_plan_topup_order', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13');

DROP TABLE IF EXISTS ods.ods_Nobel_data_plan_topup_order;

RENAME TABLE ods.ods_Nobel_data_plan_topup_order_tmp TO ods.ods_Nobel_data_plan_topup_order;
"