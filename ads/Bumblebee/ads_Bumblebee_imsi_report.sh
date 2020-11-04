#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh


clickhouse-client --user $user --password '' --multiquery --multiline -q"
create table if not exists ads.ads_Bumblebee_imsi_report
(imsi String,
usag float)
Engine=MergeTree
order by imsi
SETTINGS index_granularity = 8192;

TRUNCATE TABLE ads.ads_Bumblebee_imsi_report;

INSERT INTO ads.ads_Bumblebee_imsi_report SELECT
    imsi,
    round(((sum(download) / 1024) / 1024) / 1024, 2) AS usage
FROM ods.ods_Bumblebee_imsi_transaction_cdr_raw AS obitcr
WHERE (imsi IN ('454006109074792', '454006109074793', '454006109074800', '454006109074803', '454006109074808', '454006109074810')) AND (end_time > '2020-08-06 00:00:00')
GROUP BY imsi
UNION ALL
SELECT
    '合计',
    round(((sum(download) / 1024) / 1024) / 1024, 2) AS usage
FROM ods.ods_Bumblebee_imsi_transaction_cdr_raw AS obitcr
WHERE (imsi IN ('454006109074792', '454006109074793', '454006109074800', '454006109074803', '454006109074808', '454006109074810')) AND (end_time > '2020-08-06 00:00:00')
UNION ALL
SELECT
    '剩余',
    800 - round(((sum(download) / 1024) / 1024) / 1024, 2) AS usage
FROM ods.ods_Bumblebee_imsi_transaction_cdr_raw AS obitcr
WHERE (imsi IN ('454006109074792', '454006109074793', '454006109074800', '454006109074803', '454006109074808', '454006109074810')) AND (end_time > '2020-08-06 00:00:00');
"