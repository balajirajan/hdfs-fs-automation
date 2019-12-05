#!/bin/bash

RML=`rm -rf $lock_file`
if [[ $? -ne 0 ]]
then
	echo "Script Completed! But Couldn't Remove The Lock File Due To $RML"
	exit 0
else
	echo "Script Completed! Lock File Removed Successfully.."
	exit 0
fi
