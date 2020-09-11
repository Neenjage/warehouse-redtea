#!/bin/bash

clickhouse-client -u$1 --multiquery -q"
INSERT INTO table ods_Lagrange_Bayer.Bayer_card
(id, cid, iccid, puk, imsi, msisdn, status, create_time, last_update_time, import_time)
SELECT id, cid, iccid, puk, imsi, msisdn, status, create_time, last_update_time,today() from
mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Bayer', 'card', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
where id > (select max(id) max_c from ods_Lagrange_Bayer.Bayer_card);
"
