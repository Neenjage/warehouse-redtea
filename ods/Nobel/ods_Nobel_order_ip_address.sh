#!/bin/bash

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS ods.ods_Nobel_order_ip_address
ENGINE = MergeTree
ORDER BY id AS
SELECT *
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'order_ip_address', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
";

clickhouse-client --user $user --password $password --multiquery --multiline -q"
INSERT INTO ods.ods_Nobel_order_ip_address SELECT *
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'order_ip_address', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
WHERE id >
(
    SELECT max(id)
    FROM ods.ods_Nobel_order_ip_address
)
";