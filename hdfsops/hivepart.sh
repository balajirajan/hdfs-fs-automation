#!/bin/bash
Exchanges="googleadx mopub nexage openx scaleout smaato inneractive axonix"
#Logtypes="Bidder BidsWons AdLogs"
Logtypes="bidrequestlog_nobids"
Data_Dir="/data/bidlogdata"
MinExchange="googleadx others"
DB=adlogs
#CurrentPath=$( cd $(dirname $0) ; pwd -P )
CurrentPath=/data/scripts/hive/nobids
logfile=$CurrentPath/daily-tool.log


# FUNCTION TO PARTITION HIVE TABLE
partition() {
	source partition-mod.sh
}


# FUNCTION TO CREATE/REMOVE LOCK FILE
rmlockfile(){
	source rmlockfile-mod.sh
}

# FUNCTION TO CREATE TIMESTAMP
occured() {
	date +"%Y-%m-%d %H:%M:%S %Z"
}


###sendmail
sendmail() {
        mailstatus=$1
        if [ $1 = Error ]
        then
                echo "Failed for the day $CurrentDate - At $(occured). Because $endlog" | mail -s "Failed To Push $CurrentDate Data" "balaji@near.co"
        elif [ $1 = Success ]
        then
                echo "Finished for the day $CurrentDate - At $(occured)." | mail -s "Completed Pushing $CurrentDate Data " "balaji@near.co"
        else
                echo "Status Unknown $1 for the day $CurrentDate - At $(occured). Please Check The Log File Immediately!" | mail -s "Unknown Error $1 While Pushing $CurrentDate Data" "balaji@near.co"
        fi
}


##Lockfile creation
lock_file=$CurrentPath/daily-tool.lock
if [ ! -e "$lock_file" ]
        then
                echo "Lock File Doesn't Exists, Creating One Now..."
        touch "$lock_file"
    else
        endlog="Lock File Exists! So The Previous Job Didn't Finished. Sleeping For 3m Now..."
        echo $endlog
        #sleep 5m
        exit 1
fi


### Status check on whether the hdfs data available.

status=`cat $logfile | awk 'END {print}' | awk '{if (NF != 0 ) print $(NR) ; else print "EMPTY"}'`
echo "The Current Status is: $status , Continuing..."

if [ $status = 'Finished' ]
then

	LastDate=`cat $logfile | awk 'END {print}' | awk '{print $(NR+1)}'`
	CurrentDate=`date +"%Y-%m-%d" -d "$LastDate + 1 day"`

	OnlineDate=`hadoop fs -ls /data/bidlogdata/ | grep -v items | awk '{print $NF}' | awk -F\/ '{print $NF}' | awk '/^([0-9]{2,4}-?){3}/{ print $0 }' | awk 'END {print $NF}'`

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
    				
    				echo "partition $Data_Dir $CurrentDate $Exchanger $Logtyper $DB"
						partition $Data_Dir $CurrentDate $Exchanger $Logtyper $DB
				done
			done

			echo "Partitioning for the day $CurrentDate completed successfully..."
			

			endlog="Finished $CurrentDate Data Occured At $(occured)"
			echo $endlog >> $logfile

		fi

		rmlockfile

	else

		endlog="Error: The Day To Be Processed $CurrentDate Is The Same Or Greater Than Available Date $OnlineDate In HDFS. Now sleeping for 1 min before next check..."
		echo $endlog
		sleep 1m
		rmlockfile
	fi

elif [ $status = 'Starting' ]
then

	LastDate=`cat $logfile | awk 'END {print}' | awk '{print $(NR+1)}'`
	CurrentDate=`date +"%Y-%m-%d" -d "$LastDate"`

	endlog="Error: The Job For The Day $CurrentDate Didn't Finished Properly - Occured At $(occured). Check The Log File Immediately! Now sleeping for 1 min before next check..."
	echo $endlog

	sendmail Error
	sleep 1m
	rmlockfile

elif [ $status = 'EMPTY' ]
then
	# CurrentDate=`date +"%Y-%m-%d"`
	CurrentDate="2017-07-25"

	# Set this to True, if partition is needed
	DoPartition="True"

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
	    				echo "partition $Data_Dir $CurrentDate $Exchanger $Logtyper $DB"
						partition $Data_Dir $CurrentDate $Exchanger $Logtyper $DB.
					done
				done

				echo "Partitioning for the day $CurrentDate completed successfully..."
			else
				echo "Partitioning skipped due to settings..."
			fi


		

			endlog="Finished $CurrentDate Data Occured At $(occured)"
			echo $endlog >> $logfile

			sendmail Success
		fi

		rmlockfile

	else

		endlog="Error: The Day To Be Processed $CurrentDate Is The Same Or Greater Than Available Date $OnlineDate In HDFS. Now sleeping for 1 min before next check..."
		echo $endlog
		sleep 10s
		rmlockfile
	fi

else
	endlog="Error: Cannot Understand The Status $status - Occured At $(occured). Check The Log File Immediately!"
	echo $endlog
	sendmail Error
	sleep 10s
	rmlockfile
fi
