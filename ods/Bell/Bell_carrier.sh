#!/bin/bash

clickhouse-client -u$1 --multiquery -q"
INSERT INTO table ods_Bell_Nobel.Bell_carrier
(id, name, status, remark, import_time)
SELECT id, name, status, remark,today() from
mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Bell', 'carrier', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
"
