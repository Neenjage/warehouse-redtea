#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists ods.ods_Nobel_topup_package_order_tmp;

CREATE TABLE ods.ods_Nobel_topup_package_order_tmp
ENGINE=MergeTree
order by id as
SELECT
      id,
      order_no,
      out_order_no,
      topup_package_id,
      topup_package_name,
      topup_package_price,
      order_price,
      status,
      currency_id,
      payment_methods_id,
      create_time,
      update_time,
      order_status,
      refund_reason,
      refund_time,
      amount,
      user_id,
      source_type,
      update_time as effective_time,
      toDateTime('2105-12-31 23:59:59') AS invalid_time
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel','topup_package_order','redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13');

DROP TABLE IF EXISTS ods.ods_Nobel_topup_package_order;

RENAME TABLE ods.ods_Nobel_topup_package_order_tmp TO ods.ods_Nobel_topup_package_order;
"