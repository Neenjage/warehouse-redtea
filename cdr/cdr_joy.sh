#!/bin/bash
source /etc/profile
date=`date +%Y%m%d`
#date='20190928'
#date=`date --date='8 hour' +%Y%m%d%H%M`
file="/data/joy"
rgx="rt_cdrs_data_mvne01_daily_${date}.csv"
ff="/data/joy/rt_cdrs_data_mvne01_daily_${date}.csv"

lftp -u redtea_dur,3YhdxvZQWGWZ sftp://172.31.18.221:2222 <<!
cd /dur_save/joy_save
lcd /data/joy
get $rgx
!

java -jar /home/ops/ods/import_Newton/importData.jar $ff
echo $ff
