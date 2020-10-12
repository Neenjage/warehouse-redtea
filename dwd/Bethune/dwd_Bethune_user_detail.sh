source  /home/ops/warehouse-redtea/config/config.sh

import_time=date +%F

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
create table dwd.dwd_Bethune_user_detail_tmp
Engine=MergeTree
order by user_id as
select
  user.id as user_id,
  user.telephone as user_telephone,
  user.status as user_status,
  user.recommend_user,
  user.create_time,
  user.login_time,
  user.is_valid,
  user_device.imei,
  user_device.device_id,
  user_device.model,
  lower(user_device.brand) as brand
from
(select
  id,
  telephone,
  status,
  recommend_user,
  create_time,
  login_time,
  is_valid
from
ods.ods_Bethune_user) user
left join
(select
  user_id,
  max(imei) as imei,
  max(device_id) as device_id,
  max(model) as model,
  max(brand) as brand
from ods.ods_Bethune_user_device
group by user_id) user_device on user.id = user_device.user_id
"


clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table dwd.dwd_Bethune_user_detail
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
rename table dwd.dwd_Bethune_user_detail_tmp to dwd.dwd_Bethune_user_detail
"

