#!/bin/bash

#Created on      :May 16, 2014
#Author          :Rudra Prasad
#Purpose         :Lock at successful db query execution and upload to DFE, Archive and Application Server for a day.
#                 Also send alert mails for success and failure in ETL process.

script_path="/home/python/scripts/Inactive_Base"
logpath="$script_path/Logs"
errlog="$logpath/etl.log.err"
jobpath="$script_path/src"
job="Inactive_Base_ETL.py"

cd $jobpath

if [ -e "$jobpath/$job" ]
then
    python $jobpath/$job >/dev/null 2>$errlog
else
    echo "ERROR :`date +'%Y-%b-%d %H:%M:%S'` :Job Not Found [$jobpath/$job]" >$e
fi

#END
