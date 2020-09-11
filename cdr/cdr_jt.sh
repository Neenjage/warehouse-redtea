#!/bin/bash
source /etc/profile
date=`date +%Y%m%d%H%M`
#date=`date --date='8 hour' +%Y%m%d%H%M`
file="/data/jt/"
cc=""

for fileName in `ls $file`
        do
		ss=${fileName##*-}
                bb=${ss:0:8}${ss:9:4}
                if [[ $bb > $cc ]] ; then
                   cc=$bb
                fi
        done
dcc="${cc:0:8} ${cc:8:2}:${cc:10:2}"

while [[ $cc < $date ]]
do
	dcc=`date -d "$dcc 1 minute" +"%Y%m%d %H:%M"`
	cc="${dcc:0:8}${dcc:9:2}${dcc:12:2}"
	dd="${dcc:0:8}_${dcc:9:2}${dcc:12:2}"
	rgx="*-${dd}*.csv.processed"
#sftp -oIdentityFile=/data/rsa/mvno_rt_245 -oPort=8922 mvno_rt_v28h@203.160.90.245 <<!
ftp -n <<!
open jtglobal.ftpuk.net
user Redtea 3mC0488W
binary
prompt
cd /CDRs
lcd /data/jt
mget $rgx
close
bye
!
	for ff in `ls $file$rgx`
		do	echo $ff
			java -jar /home/ops/ods/import_Newton/importData.jar $ff
		done
done
