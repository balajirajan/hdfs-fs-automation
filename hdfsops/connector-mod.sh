#!/bin/bash

# FUNCTION TO PARTITION HIVE TABLE
partition() {
	source partition-mod.sh
}

# FUNCTION TO INSERT HIVE TABLE DATA INTO ELASTICSEARCH
insert() {
	source insertion-mod.sh
}

# FUNCTION TO CREATE/REMOVE LOCK FILE
rmlockfile(){
	source rmlockfile-mod.sh
}

# FUNCTION TO CREATE TIMESTAMP
occured() {
	date +"%Y-%m-%d %H:%M:%S %Z"
}

### Code Starts Here ###

# MAIN FUNCTION TO BE CALLED
main() {
	source main-mod.sh
}

if [[ "$#" -ne 3 ]]; then
    echo "Illegal number of parameters"
    echo "This script needs THREE parameters to function properly"
    echo "(1) -=> Non-GoogleAdx Database"
    echo "(2) -=> GoogleAdx Database"
    echo "(3) -=> ElasticSearch Database"
    echo "Now, Exiting.."
else
	main $1 $2 $3
fi
