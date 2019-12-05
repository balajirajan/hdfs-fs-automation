#!/bin/bash

Exchanges="googleadx mopub nexage openx scaleout smaato inneractive"
Logtypes="Bidder BidsWons AdLogs"
Data_Dir="/data/datain"
MinExchange="googleadx others"
DB=$1
GOOGLEADX_DB=$2
ES_DB=$3
CurrentPath=$( cd $(dirname $0) ; pwd -P )
logfile=$CurrentPath/daily-tool.log

sendmail() {
	mailstatus=$1
	if [ $1 = Error ]
	then
		echo "The Hive to ElasticSearch Insertion Failed for the day $CurrentDate - At $(occured). Because $endlog" | mail -s "Failed To Push $CurrentDate Data To ElasticSearch" "balaji@near.co, gowtham@near.co"
	elif [ $1 = Success ]
	then
		echo "The Hive to ElasticSearch Insertion Finished for the day $CurrentDate - At $(occured)." | mail -s "Completed Pushing $CurrentDate Data To ElasticSearch" "balaji@near.co, gowtham@near.co"
	else
		echo "The Hive to ElasticSearch Insertion Status Unknown $1 for the day $CurrentDate - At $(occured). Please Check The Log File Immediately!" | mail -s "Unknown Error $1 While Pushing $CurrentDate Data To ElasticSearch" "balaji@near.co, gowtham@near.co"
	fi
}

databalance() {
	source databalance.sh
}

if [ ! -e "$logfile" ]
	then
		echo "Log File Doesn't Exists, Creating One Now..."
    	touch "$logfile"
    else
    	echo "Log File Exists, Continuing..."
fi

lock_file=$CurrentPath/daily-tool.lock
if [ ! -e "$lock_file" ]
	then
		echo "Lock File Doesn't Exists, Creating One Now..."
    	touch "$lock_file"
    else
    	endlog="Lock File Exists! So The Previous Job Didn't Finished. Sleeping For 24h Now..."
    	echo $endlog
    	sendmail Error
    	sleep 24h
    	exit 1
fi

status=`cat $logfile | awk 'END {print}' | awk '{if (NF != 0 ) print $(NR) ; else print "EMPTY"}'`
echo "The Current Status is: $status , Continuing..."

if [ $status = 'Finished' ]
then

	LastDate=`cat $logfile | awk 'END {print}' | awk '{print $(NR+1)}'`
	CurrentDate=`date +"%Y-%m-%d" -d "$LastDate + 1 day"`

	OnlineDate=`hadoop fs -ls /data/datain/ | grep -v items | awk '{print $NF}' | awk -F\/ '{print $NF}' | awk '/^([0-9]{2,4}-?){3}/{ print $0 }' | awk 'END {print $NF}'`

	CurrentTS=$(date -d $CurrentDate +%s)
	OnlineTS=$(date -d $OnlineDate +%s)

	if [ $OnlineTS -gt $CurrentTS ]
	then

		# CHECK IF THE DATA FOR THE DATE IS PRESENT IN HDFS
		hadoop fs -test -e $Data_Dir/$CurrentDate
		if [[ $? -ne 0 ]]
		then
			# NO DATA FOUND FOR THE DAY :(
    		endlog="From:$0\tType:Error\tmessage:No logs are present for the day $CurrentDate"
    		echo $endlog
			sendmail Error

    	else
			startlog="Starting $CurrentDate Data Occured At $(occured)"
			echo $startlog >> $logfile

			for Exchanger in $Exchanges
			do
  				for Logtyper in $Logtypes
  				do
    				if [ $Exchanger = googleadx ]
					then
						echo "partition $Data_Dir $CurrentDate $Exchanger $Logtyper $GOOGLEADX_DB"
						partition $Data_Dir $CurrentDate $Exchanger $Logtyper $GOOGLEADX_DB
					else
						echo "partition $Data_Dir $CurrentDate $Exchanger $Logtyper $DB"
						partition $Data_Dir $CurrentDate $Exchanger $Logtyper $DB
					fi
				done
			done

			echo "Partitioning for the day $CurrentDate completed successfully..."
			for MinExchanger in $MinExchange
			do
				for Logtyper in $Logtypes
				do
					if [ $MinExchanger = googleadx ]
					then
						echo "Insert $CurrentDate $MinExchanger $Logtyper $GOOGLEADX_DB"
  						insert $CurrentDate $MinExchanger $Logtyper $GOOGLEADX_DB
  					else
  						echo "Insert $CurrentDate $MinExchanger $Logtyper $DB"
  						insert $CurrentDate $MinExchanger $Logtyper $DB
  					fi
				done
			done

			endlog="Finished $CurrentDate Data Occured At $(occured)"
			echo $endlog >> $logfile

			databalance
			sendmail Success
		fi

		rmlockfile

	else

		endlog="Error: The Day To Be Processed $CurrentDate Is The Same Or Greater Than Available Date $OnlineDate In HDFS. Now sleeping for 1 hour before next check..."
		echo $endlog
		sleep 1h
		rmlockfile
	fi

elif [ $status = 'Starting' ]
then

	LastDate=`cat $logfile | awk 'END {print}' | awk '{print $(NR+1)}'`
	CurrentDate=`date +"%Y-%m-%d" -d "$LastDate"`

	endlog="Error: The Job For The Day $CurrentDate Didn't Finished Properly - Occured At $(occured). Check The Log File Immediately! Now sleeping for 5 hours before next check..."
	echo $endlog

	sendmail Error
	sleep 5h
	rmlockfile

elif [ $status = 'EMPTY' ]
then
	# CurrentDate=`date +"%Y-%m-%d"`
	CurrentDate="2016-10-12"

	# Set this to True, if partition is needed
	DoPartition="False"

	OnlineDate=`hadoop fs -ls $Data_Dir | grep -v items | awk '{print $NF}' | awk -F\/ '{print $NF}' | awk '/^([0-9]{2,4}-?){3}/{ print $0 }' | awk 'END {print $NF}'`

	CurrentTS=$(date -d $CurrentDate +%s)
	OnlineTS=$(date -d $OnlineDate +%s)

	if [ $OnlineTS -gt $CurrentTS ]
	then

		# CHECK IF THE DATA FOR THE DATE IS PRESENT IN HDFS
		hadoop fs -test -e $Data_Dir/$CurrentDate
		if [[ $? -ne 0 ]]
		then
			# NO DATA FOUND FOR THE DAY :(
    		endlog="From:$0\tType:Error\tmessage:No logs are present for the day $CurrentDate"
    		echo $endlog
			sendmail Error

    	else
			startlog="Starting $CurrentDate Data Occured At $(occured)"
			echo $startlog >> $logfile

			if [ $DoPartition = True ]
			then

				for Exchanger in $Exchanges
				do
	  				for Logtyper in $Logtypes
	  				do
	    				if [ $Exchanger = googleadx ]
						then
							echo "partition $Data_Dir $CurrentDate $Exchanger $Logtyper $GOOGLEADX_DB"
							partition $Data_Dir $CurrentDate $Exchanger $Logtyper $GOOGLEADX_DB
						else
							echo "partition $Data_Dir $CurrentDate $Exchanger $Logtyper $DB"
							partition $Data_Dir $CurrentDate $Exchanger $Logtyper $DB
						fi
					done
				done

				echo "Partitioning for the day $CurrentDate completed successfully..."
			else
				echo "Partitioning skipped due to settings..."
			fi


			for MinExchanger in $MinExchange
			do
				for Logtyper in $Logtypes
				do
					if [ $MinExchanger = googleadx ]
					then
						echo "Insert $CurrentDate $MinExchanger $Logtyper $GOOGLEADX_DB"
  						insert $CurrentDate $MinExchanger $Logtyper $GOOGLEADX_DB
  					else
  						echo "Insert $CurrentDate $MinExchanger $Logtyper $DB"
  						insert $CurrentDate $MinExchanger $Logtyper $DB
  					fi
				done
			done

			endlog="Finished $CurrentDate Data Occured At $(occured)"
			echo $endlog >> $logfile

			databalance
			sendmail Success
		fi

		rmlockfile

	else

		endlog="Error: The Day To Be Processed $CurrentDate Is The Same Or Greater Than Available Date $OnlineDate In HDFS. Now sleeping for 1 hour before next check..."
		echo $endlog
		sleep 1h
		rmlockfile
	fi

else
	endlog="Error: Cannot Understand The Status $status - Occured At $(occured). Check The Log File Immediately!"
	echo $endlog
	sendmail Error
	sleep 24h
	rmlockfile
fi
