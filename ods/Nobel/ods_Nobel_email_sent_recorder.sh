#!/bin/bash

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE ods.ods_Nobel_email_sent_recorder
ENGINE = MergeTree
ORDER BY id AS
SELECT *
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'email_sent_recorder', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
";


clickhouse-client --user $user --password $password --multiquery --multiline -q"
INSERT INTO ods.ods_Nobel_email_sent_recorde SELECT *
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'email_sent_recorder', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
WHERE id >
(
    SELECT max(id)
    FROM ods.ods_Nobel_email_sent_recorde
)
";