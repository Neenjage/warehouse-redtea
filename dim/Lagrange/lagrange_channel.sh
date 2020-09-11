#!/bin/bash

clickhouse-client -u --multiquery -q"
INSERT INTO table ods_Lagrange_Bayer.channel
(id, app_id, app_secret, app_secret2, company_name, money, valuing_date, balance, callback_url, sms_price, create_time, update_time, merchant_code, access_key, secret_key,import_time)
SELECT id, app_id, app_secret, app_secret2, company_name, money, valuing_date, balance, callback_url, sms_price, create_time, update_time, merchant_code, access_key, secret_key,today() from
mysql('lagrange-redteago-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Lagrange', 'channel', 'redtea-ro', 'Ahxee7auzaeGhoo5phien4uTah1pahja4');
"
