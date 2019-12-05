#!/bin/bash

echo "Entering ElasticSearch Insert Function..."

da=$1    # Date to be processed
ex=$2    # Exchange to be processed
lo=$3    # Log type to be processed
daba=$4  # Database to be processed
# ES_DB variable from main-mod, defines ElasticSearch database in Hive

Job_Name="$da-$ex-$lo"    # Set hadoop job name

if [ $ex = googleadx ]
then
	echo "Now, Inserting into $ES_DB.g_$lo from $daba.$lo for the day $da"
	INS=`hive -e "
		SET mapred.job.name = '$Job_Name';
		SET mapred.job.queue.name = elasticsearch;
		SET mapreduce.map.cpu.vcores = 2;
		SET mapreduce.map.memory.mb = 4096;
		SET mapred.map.tasks = 20;
		INSERT INTO TABLE $ES_DB.g_$lo select * from $daba.$lo where date='$da';" 2>&1 > /dev/null`
	if [[ $? -ne 0 ]]
	then
		echo -e "From:$0\tType:Fail\tMessage:Couldn't Insert Data of g_$lo of Day $da To ElasticSearch, Due To $INS"
	else
		echo -e "From:$0\tType:Success\tMessage:Inserted Data of g_$lo of Day $da To ElasticSearch"
	fi
else
	echo "Now, Inserting into $ES_DB.$lo from $daba.$lo for the day $da"
	INS=`hive -e "
		SET mapred.job.name = '$Job_Name';
		SET mapred.job.queue.name = elasticsearch;
		SET mapreduce.map.cpu.vcores = 2;
		SET mapreduce.map.memory.mb = 4096;
		SET mapred.map.tasks = 20;
		INSERT INTO TABLE $ES_DB.$lo select * from $daba.$lo where date='$da'" 2>&1 > /dev/null`
	if [[ $? -ne 0 ]]
	then
		echo -e "From:$0\tType:Fail\tMessage:Couldn't Insert Data of $lo of Day $da To ElasticSearch, Due To $INS"
	else
		echo -e "From:$0\tType:Success\tMessage:Inserted Data of g_$lo of Day $da To ElasticSearch"
	fi
fi
