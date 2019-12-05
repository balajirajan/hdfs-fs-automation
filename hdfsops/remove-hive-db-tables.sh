#!/bin/bash

# Check parameters
if [[ "$#" -ne 3 ]]; then
    
    echo "Illegal number of parameters..."
    echo "This script requires THREE parameters to function properly."
    echo "(1) -=> Non-GoogleAdx Database Name. | Ex: es"
    echo "(2) -=> GoogleAdx Database Name. | Ex: g_es"
    echo "(3) -=> ElasticSearch Database Name. | Ex: elasticsearch"
    echo "Now, Exiting.."
    exit 1

else

    # Hive table specific data
    DB=$1                # Pass the Non-GoogleAdx database as second parameter. Ex: es
    GOOGLEADX_DB=$2      # Pass the GoogleAdx database as third parameter. Ex: g_es
    ES_DB=$3             # Pass the ElasticSearch database as fourth parameter. Ex: elasticsearch

    # Database & Tables removal starts here
    echo -e "Dropping the database $DB.."
    hive -e "DROP DATABASE IF EXISTS $DB CASCADE;"

    echo -e "Dropping the database $GOOGLEADX_DB.."
    hive -e "DROP DATABASE IF EXISTS $GOOGLEADX_DB CASCADE;"

    echo -e "Dropping the database $ES_DB.."
    hive -e "DROP DATABASE IF EXISTS $ES_DB CASCADE;"

fi
