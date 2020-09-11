#!/bin/bash
date=`date +%Y-%m-%d`
#bdate=`date -d "+1 day" +%Y-%m-%d`
ydate=`date --date='1 day ago' +%Y-%m-%d`
today="$date 16:00:00"
yesterday="$ydate 16:00:00"

#today="2019-10-22 16:00:00"
#yesterday="2019-10-21 16:00:00"

clickhouse-client -u$1 --multiquery -q"
insert into table ods_Newton.newton_orders
(id, customer_id, data_plan_id, count, status, update_time, order_time, activate_time, login_time, end_time, imei, pay_price, refund_reason, reseller_id, original_amount, expiration_time, reseller_user, original_unit_price, pay_unit_price, actual_pay_price, product_type, bundle_enabled)
select id, customer_id, data_plan_id, count, status, update_time, order_time, activate_time, login_time, end_time, imei, pay_price, refund_reason, reseller_id, original_amount, expiration_time, reseller_user, original_unit_price, pay_unit_price, actual_pay_price, product_type, bundle_enabled from
mysql('ro-newton-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Newton', 'orders', 'redtea-ro', 'TecirEk8ph2jukapH83jcefaqAfa4Gpcg')
where update_time >= '$yesterday'and update_time < '$today'"



