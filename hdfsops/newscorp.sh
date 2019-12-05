#!/bin/bash
Data_Dir="/data/newscorp/pixelevents"
CurrentPath=/home/balajir/hdfsops/newscorp
logfile=$CurrentPath/daily-tool.log


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
	CurrentDate=`date +"%y-%m-%d" -d "$LastDate + 1 day"`
	DATE=`date +"%Y-%m-%d" -d "$LastDate + 1 day"`
	CurrentYear=`date +"%Y" -d "$LastDate + 1 day"`
	CurrentMonth=`date +"%m" -d "$LastDate + 1 day"`
	OnlineDate=`hadoop fs -ls /data/newscorp/pixelevents/ | grep -v items | awk '{print $NF}' | awk -F\/ '{print $NF}' | awk '/^([0-9]{2,4}-?){3}/{ print $0 }' | awk 'END {print $NF}'`

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
###	
    				echo "backup data $CurrentDate to s3 bucket"
    				#sudo -u hdfs hdfs dfs -get /data/newdatain/staging/$CurrentDate /disk1/backup/obidlogs/
				hadoop distcp -Dmapreduce.job.queuename=dataops -Dmapreduce.job.name=newscorp-$CurrentDate /data/newscorp/pixelevents/$CurrentDate s3a://2xA9HhRHBZhqxsUMgJRr:5qlMOqb5PRxAfnCc6uKK1TydtjKm1nQsZLEpgbEE@near-hdfs-newscorp/$CurrentYear/$CurrentMonth/$DATE/

			echo "Partitioning for the day $CurrentDate completed successfully..."
###			
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
	CurrentDate="18-11-17"
	LastDate=`cat $logfile | awk 'END {print}' | awk '{print $(NR+1)}'`
	CurrentYear=`date +"%Y" -d "$LastDate + 1 day"`
        CurrentMonth=`date +"%m" -d "$LastDate + 1 day"`
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
			
			echo "backup data $CurrentDate to S3bucket"
    		#sudo -u hdfs hdfs dfs -get /data/newdatain/staging/$CurrentDate /disk1/backup/obidlogs/
		hadoop distcp -Dmapreduce.job.queuename=dataops -Dmapreduce.job.name=newscorp-$CurrentDate /data/newscorp/pixelevents/$CurrentDate s3a://2xA9HhRHBZhqxsUMgJRr:5qlMOqb5PRxAfnCc6uKK1TydtjKm1nQsZLEpgbEE@near-hdfs-newscorp/$CurrentYear/$CurrentMonth/		
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
