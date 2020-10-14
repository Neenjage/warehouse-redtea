#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS ods.ods_Nobel_user_modification_recorder
ENGINE = MergeTree
ORDER BY id AS
SELECT *
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'user_modification_recorder', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
";

clickhouse-client --user $user --password $password --multiquery --multiline -q"
INSERT INTO ods.ods_Nobel_user_modification_recorder
SELECT *
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'user_modification_recorder', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
WHERE id >
(
    SELECT max(id) AS max_c
    FROM ods.ods_Nobel_user_modification_recorder
)
";
