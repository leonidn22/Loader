#!/usr/bin/env /usr/local/bin/python2.7

##############User defined parameters:###########

#root dir of the project
root_path='/opt/allot/vftrk/'
# loader log file
loader_log='/opt/allot/vftrk/logs/loader.log'

csv_file_prefix='vftrk'
ipquery_csv_file_path='/opt/allot/vftrk/csv/'

copyCDRV7 = {
#root dir of the project
     "root_path":"/opt/allot/vftrk/",
#data from Data Mediator
    "data_dir": "/data1/vftrk/",
# exceptions
    "exceptions": "/data1/vftrk/exceptions/%s",
# rejected
    "rejected": "/data1/vftrk/rejected/%s",
# loader log file
    "loader_log": "/opt/allot/vftrk/logs/loader.log",
    "csv_file_prefix": "vftrk",
    "ipquery_csv_file_path": "/opt/allot/vftrk/csv/",
    "pattern": "*CONV_V7.csv.gz",
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

aggregation = {
    "01-insert2APP": """
        insert /*+direct*/ into data.CDR_Application (Application)
        select distinct(Application) from data.CDR as CD
        where Start_hour = '%s' and Application is not null
        and not exists( select 1 from data.CDR_Application CA where CA.Application = CD.Application)
        union
        select distinct(Application) from data.HDR as CD
        where Start_hour = '%s' and Application is not null
        and not exists( select 1 from data.CDR_Application CA where CA.Application = CD.Application)
    """,
    "02-insert2CDR_AGG": """
        insert /*+direct*/ into data.CDR_AGG
        select IP_source , IP_dest , Application_id ,Start_hour ,sum(Bytes_in), sum(Bytes_out)from (
                    select CDR.IP_source  ,CDR.IP_dest,APP.Application_id, CDR.Start_hour
                    ,Bytes_in, Bytes_out from data.CDR CDR
                         , data.CDR_Application APP
                    where CDR.Start_hour = '%s'
                    and CDR.Application = APP.Application
                    and CDR.IP_source is not null and CDR.IP_dest is not null
                    union all
                    select CDR.IP_source ,CDR.IP_dest,APP.Application_id, CDR.Start_hour
                    ,Bytes_in, Bytes_out from data.HDR CDR
                         , data.CDR_Application APP
                    where CDR.Start_hour = '%s'
                    and CDR.Application = APP.Application
                    and CDR.IP_source is not null and CDR.IP_dest is not null
                    ) inner_union
        group by Start_hour ,IP_source , IP_dest , Application_id

    """
}

