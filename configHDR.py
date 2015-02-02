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
    "data_dir": "/data1/vftrk/",
# exceptions
    "exceptions": "/data1/vftrk/exceptions/%s",
# rejected
    "rejected": "/data1/vftrk/rejected/%s",
# loader log file
    "loader_log": "/opt/allot/vftrk/logs/loader.log",
    "csv_file_prefix": "vftrk",
    "ipquery_csv_file_path": "/opt/allot/vftrk/csv/",
    "pattern": "*HDR_V7.csv.gz",
    "copyCMD": """
    copy data.HDR(Start_time,SubscriberID filler varchar(128),SessionKey filler int
    ,f1 filler varchar, IP_Source as INET_ATON(regexp_substr(f1, '(^.+:)?(\d|\.)+', 2))
    , client_port filler varchar(50)
    ,f2 filler varchar, IP_Dest as INET_ATON(regexp_substr(f2, '(^.+:)?(\d|\.)+', 2))
    , server_port filler varchar(50)
    , Application ,
    HTTPMethod filler varchar(8) enclosed by '"',
    RequestHeader_Host filler varchar(50) enclosed by '"',
    URI_in filler varchar(255) enclosed by '"',
    RequestHeader_User_Agent_in filler varchar(255) enclosed by '"',
    RequestHeader_Referer_in filler varchar(255) enclosed by '"',
    ResponseHeader_Content_Type filler varchar(255) enclosed by '"',
    DownloadContentLength filler varchar enclosed by '"',
    UploadContentLength filler varchar enclosed by '"',
    HTTPResponseCode filler varchar(20) enclosed by '"',
    ServerInitialResponseTime filler varchar enclosed by '"',
    Duration filler varchar enclosed by '"',
    Bytes_in, Bytes_out,
    RequestHeader_DNT_x_do_not_track_in filler varchar(255) enclosed by '"',
    L5Protocol_in filler varchar(16) enclosed by '"')
    from '%s' gzip
    WITH FILTER data.apst_filter() ESCAPE AS '`'
    delimiter ',' enclosed '"' skip 1
	direct
    EXCEPTIONS '%s' REJECTED DATA '%s'
        """
    }

aggregation = {
    "01-insert2APP": """
        insert /*+direct*/ into data.CDR_Application (Application)
        select distinct(Application) from data.HDR as CD
        where Start_hour = '%s' and Application is not null
        and not exists( select 1 from data.CDR_Application CA where CA.Application = CD.Application)
    """,
    "02-insert2CDR_AGG": """
        insert /*+direct*/ into data.CDR_AGG
        select CDR.IP_source,CDR.IP_dest,APP.Application_id, CDR.Start_hour
        ,sum(Bytes_in), sum(Bytes_out)
        from data.HDR CDR, data.CDR_Application APP
        where Start_hour = '%s'
        and CDR.Application = APP.Application
        and CDR.IP_source is not null and CDR.IP_dest is not null
        group by CDR.Start_hour ,CDR.IP_source , CDR.IP_dest , APP.Application_id
    """
}


