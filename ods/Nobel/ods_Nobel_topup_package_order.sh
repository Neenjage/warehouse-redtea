#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS ods.ods_Nobel_topup_package_order
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

drop table if exists ods.ods_Nobel_topup_package_order_temp;

CREATE TABLE ods.ods_Nobel_topup_package_order_temp
ENGINE = MergeTree
ORDER BY id AS
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
    effective_time,
    if(b.id = 0, a.invalid_time, b.update_time) AS invalid_time
FROM ods.ods_Nobel_topup_package_order AS a
ANY LEFT JOIN
(
    SELECT
        id,
        update_time
    FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel','topup_package_order','redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
    WHERE update_time >
    (
        SELECT max(update_time)
        FROM ods.ods_Nobel_topup_package_order
    )
) AS b USING (id);

DROP TABLE IF EXISTS ods.ods_Nobel_topup_package_order;

RENAME TABLE ods.ods_Nobel_topup_package_order_temp TO ods.ods_Nobel_topup_package_order;

INSERT INTO ods.ods_Nobel_topup_package_order
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
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel','topup_package_order','redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
WHERE update_time >
(
    SELECT max(update_time)
    FROM ods.ods_Nobel_topup_package_order
);
"