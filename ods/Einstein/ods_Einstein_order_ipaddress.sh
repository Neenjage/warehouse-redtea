#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password '' --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS ods.ods_Einstein_order_ipaddress
ENGINE = MergeTree
ORDER BY id as
select
    id,
    ip,
    order_no,
    province,
    address,
    create_time
FROM mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'order_ipaddress', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c');

INSERT INTO  table ods.ods_Einstein_order_ipaddress
select
    id,
    ip,
    order_no,
    province,
    address,
    create_time
FROM mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'order_ipaddress', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c')
WHERE id >
(
  SELECT
    MAX(id)
  FROM ods.ods_Einstein_order_ipaddress
);
"