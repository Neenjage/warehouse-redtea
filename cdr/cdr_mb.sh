#!/bin/bash
source /etc/profile
today=`date +%Y-%m-%d`
#today="2019-09-11"
yesterday=`date --date='1 day ago' +%Y-%m-%d`
#yesterday="2019-09-10"
file="/data/mb/"

local_date=`date --date='8 hour' +%Y-%m-%d`
date=`date --date='8 hour' +%Y%m%d`
folder="${file}mb_${local_date}"

if [ ! -d "$folder" ]; then
                         mkdir "$folder"
fi

source="RT01_${date}*.csv.processed"

#lftp -u redtea:17W9HC002m -e"mirror –n -I /cdr/RT01_${date}*.csv.processed /data/mb/$folder" 202.130.86.119

lftp -u redtea,17W9HC002m sftp://202.130.86.119 << EOF
cd /cdr
lcd $folder
mirror -n -I $source
bye
EOF
