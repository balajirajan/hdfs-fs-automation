#!/bin/bash

echo "Entering Partition Function..."
# GET ALL VARIABLES FROM THE PARAMTERS
Root_Dir=$1 # DATA DIRECTORY LOCATION IN HDFS
Date=$2 # DATE TO BE PROCESSED
Exchange=$3 # EXCHANGE TO BE PROCESSED
Logtype=$4 # LOG TYPE TO BE PROCESSED
Database=$5 # DATABASE WHERE THE DATA RESIDES
year=`date --date=$currentDate +%Y` #Y FOR YEAR
month=`date --date=$currentDate +%m` #m FOR MONTH
day=`date --date=$currentDate +%d` #d FOR DAY

Hour_List=`hadoop fs -ls $Root_Dir/$Date | grep -v items | awk '{print $NF}' | awk -F\/ '{print $NF}' | awk '/^([0-9]{2}){1}/{ print $0 }'`
# LOOP OVER HOURS TO GET EVERY HOUR THAT NEEDS TO BE PROCESSED
for Hour in $Hour_List
do
	# NOW FOR EVERY HOUR PARTITION THE CORRESPONDING HIVE TABLE
	CompletePath=$Root_Dir/$Date/$Hour/$Exchange/$Logtype/
	hadoop fs -test -e $CompletePath # CHECK IF ANY LOGS FOUND INSIDE THE DIRECTORY
	if [[ $? -ne 0 ]]
	then
		# NO LOGS FOUND FOR THIS HOUR OF EXCHANGE/LOG TYPE :(
		echo -e "From:$0\tType:Miss\tmessage:No logs are present for exchange $Exchange and hour $Hour"
	else
		echo -e "From:$0\tType:Hit\tmessage:logs are present for exchange $Exchange and hour $Hour"
			MSG=`hive -e "
			USE $Database;
			ALTER TABLE $Logtype DROP IF EXISTS PARTITION(date='$Date', hour='$Hour', exchangename='$Exchange', logtype='$Logtype');
			ALTER TABLE  $Logtype 
			ADD PARTITION(date='$Date', hour='$Hour', exchangename='$Exchange', logtype='$Logtype') 
			LOCATION '$CompletePath';" 2>&1 > /dev/null`
		if [[ $? -ne 0 ]]
		then
			echo -e "From:$0\tType:Fail\tMessage:Did not add partition $MSG"
		else
	   	  	echo -e "From:$0\tType:Success\tMessage:Partitioning completed for: $CompletePath."
	   	fi
	fi
done
