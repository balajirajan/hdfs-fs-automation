#!/bin/bash

# Check parameters
if [[ "$#" -ne 5 ]]; then
    
    echo "Illegal number of parameters..."
    echo "This script requires FIVE parameters to function properly."
    echo "(1) -=> HDFS root directory path. | Ex: /data/datain/"
    echo "(2) -=> Non-GoogleAdx Database Name. | Ex: es"
    echo "(3) -=> GoogleAdx Database Name. | Ex: g_es"
    echo "(4) -=> ElasticSearch Database Name. | Ex: elasticsearch"
    echo "(5) -=> ElasticSearch Master/Client Node's IP Address. | Ex: 10.125.138.164"
    echo "Now, Exiting.."
    exit 1

else

    # Hive table specific data
    root_dir=$1          # Pass the HDFS directory as first parameter. Ex: /data/datain/
    DB=$2                # Pass the Non-GoogleAdx database as second parameter. Ex: es
    GOOGLEADX_DB=$3      # Pass the GoogleAdx database as third parameter. Ex: g_es
    ES_DB=$4             # Pass the ElasticSearch database as fourth parameter. Ex: elasticsearch
    ES_MASTER_IP=$5      # Pass the ElasticSearch master/client node's IP address. Ex: 10.125.138.164

    # Database & Tables creation starts here
    hive -e "

    CREATE DATABASE IF NOT EXISTS $DB;
    USE $DB;
    CREATE EXTERNAL TABLE   IF NOT EXISTS bidder (
        video_imp map<string,string>,
        nobid_reason string,
        id string,
        app map<string,string>,
        site map<string,string>,
        geo map<string,string>,
        istdt string,
        bcat string,
        imp map<string,string>,
        device map<string,string>,
        at string,
        istts bigint,
        user map<string,string>,
        response map<string,string>,
        aspk_data map<string,string>,
        user_tag string,
        tg_id string,
        exp_id string
        )
        PARTITIONED BY (date string,hour string, exchangename string, logtype string) 
        ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' STORED AS TEXTFILE 
        LOCATION '$root_dir';
    ALTER TABLE bidder
        SET SERDEPROPERTIES ( 'ignore.malformed.json' = 'true');
    
    CREATE EXTERNAL TABLE IF NOT EXISTS bidswons (
        id string,
        app map<string, string>,
        adid string,
        price string,
        istdt string,
        ag_id string,
        istts bigint,
        pubid string
        )
        PARTITIONED BY (date string,hour string, exchangename string, logtype string) 
        ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' STORED AS TEXTFILE 
        LOCATION '$root_dir';
    ALTER TABLE bidswons  
        SET SERDEPROPERTIES ( 'ignore.malformed.json' = 'true');
    
    CREATE EXTERNAL TABLE IF NOT EXISTS adlogs (
        app map<string, string>,
        lon string,
        aspkid string,
        istdt string,
        ag_id string,
        aspk_sid array<string>,
        id string,
        clientid string,
        adid string,
        event string,
        device map<string, string>,
        istts bigint,
        creative_size string,
        aspk_data string,
        lat string,
        pubid string,
        fraud string
        )
        PARTITIONED BY (date string,hour string, exchangename string, logtype string) 
        ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' STORED AS TEXTFILE 
        LOCATION '$root_dir';
    ALTER TABLE adlogs 
        SET SERDEPROPERTIES ( 'ignore.malformed.json' = 'true');

    CREATE DATABASE IF NOT EXISTS $GOOGLEADX_DB;
    USE $GOOGLEADX_DB;
    CREATE EXTERNAL TABLE IF NOT EXISTS bidder (
        publisher_category map<string,string>,
        app map<string, string>,
        hyperlocal_data_auto_country string,
        hyperlocal_data_auto_lon string,
        istdt string,
        imp map<string, string>,
        hyperlocal_data_auto_lat string,
        ip string,
        id string,
        nobid_reason string,
        detected_language_0 string,
        google_user_id string,
        geo_criteria_id string,
        device map<string,string>,
        user_agent string,
        istts bigint,
        cookie_age_seconds string
        )
        PARTITIONED BY (date string,hour string, exchangename string, logtype string) 
        ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' STORED AS TEXTFILE 
        LOCATION '$root_dir';
    ALTER TABLE bidder
        SET SERDEPROPERTIES ( 'ignore.malformed.json' = 'true');

    CREATE EXTERNAL TABLE IF NOT EXISTS bidswons (
        id string,
        adid string,
        price string,
        istdt string,
        istts bigint,
        app_id string	
        )
        PARTITIONED BY (date string,hour string, exchangename string, logtype string) 
        ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' STORED AS TEXTFILE 
        LOCATION '$root_dir';
    ALTER TABLE bidswons
        SET SERDEPROPERTIES ( 'ignore.malformed.json' = 'true');

    CREATE EXTERNAL TABLE IF NOT EXISTS adlogs (
        id string,
        clientid string,
        lat string,
        lon string,
        adid string,
        istdt string,
        event string,
        istts bigint,
        creative_size string,
        app_id string
        )
        PARTITIONED BY (date string,hour string, exchangename string, logtype string) 
        ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' STORED AS TEXTFILE 
        LOCATION '$root_dir';
    ALTER TABLE adlogs
        SET SERDEPROPERTIES ( 'ignore.malformed.json' = 'true');


    CREATE DATABASE IF NOT EXISTS $ES_DB;
    USE $ES_DB;
    CREATE EXTERNAL TABLE   IF NOT EXISTS g_adlogs (
        id string,
        clientid string,
        lat string,
        lon string,
        adid string,
        istdt string,
        event string,
        istts bigint,
        creative_size string,
        app_id string,
        date string,
        hour string,
        exchangename string,
        logtype string
        )
        ROW FORMAT SERDE 'org.elasticsearch.hadoop.hive.EsSerDe'
        STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
        TBLPROPERTIES (
            'es.nodes'='$ES_MASTER_IP',
            'es.mapping.names' = 'date:@timestamp',
            'es.batch.write.retry.count' = '-1',
            'es.resource'='{exchangename}-{date:YYYY.MM.dd}/{logtype}'
        );

    CREATE EXTERNAL TABLE   IF NOT EXISTS g_bidswons (
        id string,
        adid string,
        price string,
        istdt string,
        istts bigint,
        app_id string,
        date string,
        hour string,
        exchangename string,
        logtype string
        )
        ROW FORMAT SERDE 'org.elasticsearch.hadoop.hive.EsSerDe'
        STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
        TBLPROPERTIES (
            'es.nodes'='$ES_MASTER_IP',
            'es.mapping.names' = 'date:@timestamp',
            'es.batch.write.retry.count' = '-1',
            'es.resource'='{exchangename}-{date:YYYY.MM.dd}/{logtype}'
        );

    CREATE EXTERNAL TABLE   IF NOT EXISTS g_bidder (
        publisher_category map<string,string>,
        app map<string,string>,
        hyperlocal_data_auto_country string,
        hyperlocal_data_auto_lon string,
        istdt string,
        imp map<string,string>,
        hyperlocal_data_auto_lat string,
        ip string,
        id string,
        nobid_reason string,
        detected_language_0 string,
        google_user_id string,
        geo_criteria_id string,
        device map<string,string>,
        user_agent string,
        istts bigint,
        cookie_age_seconds string,
        date string,
        hour string,
        exchangename string,
        logtype string
        )
        ROW FORMAT SERDE 'org.elasticsearch.hadoop.hive.EsSerDe'
        STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
        TBLPROPERTIES (
            'es.nodes'='$ES_MASTER_IP',
            'es.batch.write.retry.count' = '-1',
            'es.batch.size.bytes' = '10mb',
            'es.batch.size.entries' = '10000',
            'es.mapping.names' = 'date:@timestamp',
            'es.resource'='{exchangename}-{date:YYYY.MM.dd}/{logtype}'
        );

    CREATE EXTERNAL TABLE   IF NOT EXISTS adlogs (
        app map<string, string>,
        lon string,
        aspkid string,
        istdt string,
        ag_id string,
        aspk_sid array<string>,
        id string,
        clientid string,
        adid string,
        event string,
        device map<string, string>,
        istts bigint,
        creative_size string,
        aspk_data string,
        lat string,
        pubid string,
        fraud string,
        date string,
        hour string,
        exchangename string,
        logtype string
        )
        ROW FORMAT SERDE 'org.elasticsearch.hadoop.hive.EsSerDe'
        STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
        TBLPROPERTIES (
            'es.nodes'='$ES_MASTER_IP',
            'es.mapping.names' = 'date:@timestamp',
            'es.mapping.exclude' = 'aspk_data',
            'es.batch.write.retry.count' = '-1',
            'es.resource'='{exchangename}-{date:YYYY.MM.dd}/{logtype}'
        );

    CREATE EXTERNAL TABLE   IF NOT EXISTS bidswons (
        id string,
        app map<string, string>,
        adid string,
        price string,
        istdt string,
        ag_id string,
        istts bigint,
        pubid string,
        date string,
        hour string,
        exchangename string,
        logtype string
        )
        ROW FORMAT SERDE 'org.elasticsearch.hadoop.hive.EsSerDe'
        STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
        TBLPROPERTIES (
            'es.nodes'='$ES_MASTER_IP',
            'es.mapping.names' = 'date:@timestamp',
            'es.batch.write.retry.count' = '-1',
            'es.resource'='{exchangename}-{date:YYYY.MM.dd}/{logtype}'
        );

    CREATE EXTERNAL TABLE   IF NOT EXISTS bidder (
        video_imp map<string,string>,
        nobid_reason string,
        id string,
        app map<string,string>,
        site map<string,string>,
        geo map<string,string>,
        istdt string,
        bcat string,
        imp map<string,string>,
        device map<string,string>,
        at string,
        istts bigint,
        user map<string,string>,
        response map<string,string>,
        aspk_data map<string,string>,
        user_tag string,
        tg_id string,
        exp_id string,
        date string,
        hour string,
        exchangename string,
        logtype string
        )
        ROW FORMAT SERDE 'org.elasticsearch.hadoop.hive.EsSerDe'
        STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
        TBLPROPERTIES (
            'es.nodes'='$ES_MASTER_IP',
            'es.mapping.names' = 'date:@timestamp',
            'es.batch.write.retry.count' = '-1',
            'es.batch.size.bytes' = '10mb',
            'es.batch.size.entries' = '10000',
            'es.resource'='{exchangename}-{date:YYYY.MM.dd}/{logtype}'
        );

    "
fi
