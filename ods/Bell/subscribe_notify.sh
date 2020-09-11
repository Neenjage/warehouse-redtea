#!/bin/bash

clickhouse-client -u$1 --multiquery -q"
INSERT INTO table ods_Bell_Nobel.subscribe_notify
(id, order_id, qr_resource_id, event_type, notify_url, create_time, result_data, import_time)
select id, order_id, qr_resource_id, event_type, notify_url, create_time, result_data, today() from 
mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Bell', 'subscribe_notify', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
where id > (select max(id) max_c from ods_Bell_Nobel.subscribe_notify);
"
