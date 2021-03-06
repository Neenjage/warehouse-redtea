#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dwd.dwd_Newton_order_detail_tmp;

create table dwd.dwd_Newton_order_detail_tmp
Engine=MergeTree
order by order_id as
select
t1.*,
reseller.name as reseller_name
from
(select
order.id as order_id,
order.customer_id,
order.data_plan_id,
order.count,
order.status,
order.update_time,
order.order_time,
order.activate_time,
order.end_time,
multiIf(
    data_plan_id='99000324',2300,
    data_plan_id='99000322',1800,
    data_plan_id='99000320',1500,
    data_plan_id='99000306',5800,
    data_plan_id='99000305',3000,
    data_plan_id='99000301',6800,
    data_plan_id='99000300',3500,
    data_plan_id='99000288',4900,
    data_plan_id='99000287',2500,
    data_plan_id='99000281',1300,
    data_plan_id='99000280',2800,
    data_plan_id='99000279',1500,
    data_plan_id='99000276',500,
    data_plan_id='99000275',2500,
    data_plan_id='99000274',4900,
    data_plan_id='99000267',1500,
    data_plan_id='99000266',2800,
    data_plan_id='99000261',3000,
    data_plan_id='99000260',1600,
    data_plan_id='99000259',2500,
    data_plan_id='99000253',2300,
    data_plan_id='99000245',1500,
    data_plan_id='99000160',6000,
    data_plan_id='99000157',4000,
    data_plan_id='99000148',9000,
    data_plan_id='99000147',600,
    data_plan_id='99000144',5800,
    data_plan_id='99000139',5800,
    data_plan_id='99000138',5800,
    data_plan_id='99000131',6000,
    data_plan_id='99000125',6000,
    data_plan_id='99000124',2500,
    data_plan_id='99000123',4000,
    data_plan_id='99000122',300,
    data_plan_id='99000120',300,
    data_plan_id='99000117',300,
    data_plan_id='99000115',400,
    data_plan_id='99000112',300,
    data_plan_id='99000106',7200,
    data_plan_id='99000104',6000,
    data_plan_id='99000103',4000,
    data_plan_id='99000101',15000,
    data_plan_id='99000100',9000,
    data_plan_id='99000099',5800,
    data_plan_id='99000096',900,
    data_plan_id='99000094',1800,
    data_plan_id='99000093',3500,
    data_plan_id='99000091',4400,
    data_plan_id='99000089',5800,
    data_plan_id='99000085',8400,
    data_plan_id='99000084',8400,
    data_plan_id='99000083',8400,
    data_plan_id='99000082',8400,
    data_plan_id='99000081',8400,
    data_plan_id='99000080',1300,
    data_plan_id='99000079',700,
    data_plan_id='99000074',1000,
    data_plan_id='99000073',700,
    data_plan_id='99000072',1100,
    data_plan_id='99000071',400,
    data_plan_id='99000069',800,
    data_plan_id='99000067',700,
    data_plan_id='99000066',1200,
    data_plan_id='99000065',900,
    data_plan_id='99000064',600,
    data_plan_id='99000063',700,
    data_plan_id='99000062',700,
    data_plan_id='99000061',700,
    data_plan_id='99000060',700,
    data_plan_id='99000059',1700,
    data_plan_id='99000058',1700,
    data_plan_id='99000056',700,
    data_plan_id='99000055',1500,
    data_plan_id='99000053',1700,
    data_plan_id='99000052',1700,
    data_plan_id='99000049',800,
    data_plan_id='99000047',700,
    data_plan_id='99000046',600,
    data_plan_id='99000045',1500,
    data_plan_id='99000044',1100,
    data_plan_id='99000043',1200,
    data_plan_id='99000041',700,
    data_plan_id='99000040',700,
    data_plan_id='99000039',1500,
    data_plan_id='99000037',2200,
    data_plan_id='99000036',700,
    data_plan_id='99000035',900,
    data_plan_id='99000034',800,
    data_plan_id='99000033',900,
    data_plan_id='99000031',5400,
    data_plan_id='99000030',8400,
    data_plan_id='99000023',700,
    data_plan_id='99000022',500,
    data_plan_id='99000019',8400,
    data_plan_id='99000018',8400,
    data_plan_id='99000016',8400,
    data_plan_id='99000014',8400,
    data_plan_id='99000010',300,
    data_plan_id='99000005',2400,
    data_plan_id='16000030',9000,
    data_plan_id='16000030',8500,
    data_plan_id='16000028',8500,
    data_plan_id='16000028',9000,
    data_plan_id='16000027',9000,
    data_plan_id='16000027',8500,
    data_plan_id='16000024',10900,
    data_plan_id='16000023',6900,
    data_plan_id='16000023',6500,
    data_plan_id='16000014',5000,
    data_plan_id='16000014',5500,
    data_plan_id='16000011',15900,
    data_plan_id='16000010',8900,
    data_plan_id='100038',1250,
    data_plan_id='100023',1700,
    data_plan_id='70172',800,
    data_plan_id='70159',1700,
    data_plan_id='70129',1700,
    data_plan_id='70127',1700,
    data_plan_id='70112',1000,
    data_plan_id='70028',2400,
    data_plan_id='70024',5000,
    data_plan_id='70022',4000,
    data_plan_id='70022',3500,
    data_plan_id='70021',2500,
    data_plan_id='70018',9000,
    data_plan_id='70003',1500,
    data_plan_id='43981',1700,
    data_plan_id='43988',1700,
    data_plan_id='43995',1700,
    data_plan_id='43708',1400,
    data_plan_id='43993',1700,
    data_plan_id='43994',1700,
    data_plan_id='43982',1700,
    data_plan_id='43970',1700,
    data_plan_id='43989',1700,
    data_plan_id='43985',1700,
    data_plan_id='43713',1700,
    data_plan_id='43986',1700,
    data_plan_id='12000037',1400,
    data_plan_id='43987',1700,
    data_plan_id='43978',1700,
    data_plan_id='43711',1100,
    data_plan_id='43972',1700,
    data_plan_id='43714',1700,
    data_plan_id='100122',1400,
    data_plan_id='43990',1700,
    data_plan_id='43992',1700,
    data_plan_id='43977',1700,
    data_plan_id='43707',800,
    data_plan_id='43976',1700,
    data_plan_id='43975',1700,
    data_plan_id='43968',1700,
    data_plan_id='43974',1700,
    data_plan_id='43848',1700,
    data_plan_id='43971',1700,
    data_plan_id='43841',1800,
    data_plan_id='43969',1700,
    data_plan_id='43706',1200,
    data_plan_id='43964',1700,
    data_plan_id='43963',1700,
    data_plan_id='43991',1700,
    data_plan_id='12000034',2000,
    data_plan_id='43967',1700,
    data_plan_id='43966',1700,
    data_plan_id='43705',600,
    data_plan_id='43962',1700,
    data_plan_id='43965',1700,
    data_plan_id='43984',1700,
    data_plan_id='43703',1200,
    data_plan_id='43710',1650,
    data_plan_id='43954',1500,
    data_plan_id='43983',1700,
    data_plan_id='43979',1700,
    data_plan_id='43980',1700,
    data_plan_id='12000039',1500,
    data_plan_id='43960',1700,
    data_plan_id='43701',800,
    data_plan_id='43709',1350,
    data_plan_id='43746',1350,
    data_plan_id='43700',600,order.pay_price
) as pay_price,
order.reseller_id,
order.expiration_time,
data_plan.name as data_plan_name,
data_plan.data_volume as data_plan_volume
from
ods.ods_Newton_orders as order
left join
ods.ods_Newton_data_plan as data_plan
on order.data_plan_id = data_plan.data_plan_id) t1
left join
ods.ods_Newton_reseller as reseller
on t1.reseller_id = reseller.id;

drop table if exists dwd.dwd_Newton_order_detail;

rename table dwd.dwd_Newton_order_detail_tmp to dwd.dwd_Newton_order_detail;
"