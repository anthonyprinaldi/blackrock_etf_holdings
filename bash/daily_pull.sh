#!/bin/bash
# pull holdings from etfs
# run daily
now=$(date +'%d-%m-%Y')
echo $now
/usr/bin/python /home/pi/dev/etf_tracking/python_scripts/daily_pull.py
echo 'DONE'
read -t 60 -p 'Press any key to continue...'
