#!/bin/bash
cd /home/python/scripts/Inactive_Base/temp
rm -rf neon.log
TODAY=`date +%d%m%Y%I%M%`


InactiveBase_FILE=${TODAY}*.csv

if [ ! -f "$InactiveBase${TODAY}*.csv" ] ;
then
    echo "Inactive_Base$Inactive_Base${TODAY}.csv is not availble" >> /home/python/scripts/Inactive_Base/temp/neon.log
fi


