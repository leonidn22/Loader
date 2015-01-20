#!/usr/bin/env /usr/local/bin/python2.7

##############User defined parameters:###########

#root dir of the project
root_path='/opt/allot/vftrk/'
# loader log file
loader_log='/opt/allot/vftrk/logs/loader.log'

csv_file_prefix='vftrk'
ipquery_csv_file_path='/opt/allot/vftrk/csv/'

copyHDRV7 = {
#root dir of the project
     "root_path":"/opt/allot/vftrk/",
#data from Data Mediator
    "data_dir": "/data/vftrk/",
# exceptions
    "exceptions": "/data/vftrk/exceptions/%s",
# rejected
    "rejected": "/data/vftrk/rejected/%s",
# loader log file
    "loader_log": "/opt/allot/vftrk/logs/loader.log",
    "csv_file_prefix": "vftrk",
    "ipquery_csv_file_path": "/opt/allot/vftrk/csv/",
    "pattern": "*HDR_V7.csv.gz",
    "copyCMD": """
    copy data.CDR(f filler varchar,Start_time, End_time filler timestamp, f1 filler varchar, IP_Source as INET_ATON(regexp_substr(f1, '(^.+:)?(\d|\.)+', 2))
    , f2 filler varchar, IP_Dest as INET_ATON(regexp_substr(f2, '(^.+:)?(\d|\.)+', 2)), Application
    ,f3 filler varchar,f4 filler varchar,f5 filler varchar,f6 filler varchar,f7 filler varchar,f8 filler varchar,f9 filler varchar,f10 filler varchar,f11 filler varchar
    ,f12 filler varchar,f13 filler varchar,f14 filler varchar,f15 filler varchar,f16 filler varchar,f17 filler varchar,f18 filler varchar,f19 filler varchar,f20 filler varchar
    , Bytes_in, Bytes_out)
    from '%s' gzip direct delimiter ',' enclosed '"' skip 1
    EXCEPTIONS '%s' REJECTED DATA '%s' ;
        """
    }



