#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password '' --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS ods.ods_Nobel_order_ip_address
ENGINE = MergeTree
ORDER BY id AS
SELECT 
    id,
    user_id,
    ip,
    address,
    country,
    province,
    city,
    order_id,
    create_time 
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'order_ip_address', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13');

INSERT INTO ods.ods_Nobel_order_ip_address
SELECT
    id,
    user_id,
    ip,
    address,
    country,
    province,
    city,
    order_id,
    create_time
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'order_ip_address', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
WHERE id >
(
    SELECT max(id)
    FROM ods.ods_Nobel_order_ip_address
);
"
