
CREATE TABLE public.CDR_LOGS_LOADER_FILES
(
        log_time timestamp DEFAULT current_timestamp::timestamptz ,
        node_name varchar(128) NOT NULL DEFAULT local_node_name(),
        file_name varchar(1024),
        unit_name varchar(512),
        file_ip   varchar(30),
        mediation_ip varchar(30),
        seq          varchar(30),
        file_date    varchar(30),
        data_type    varchar(30),
        version      varchar(30),
        success      boolean,
        error_msg     varchar(255),
        loaded_rows int,
        rejected_rows int,
        duration_sec int,
        log_date date NOT NULL DEFAULT date_trunc('DAY', current_timestamp)::date
) order by log_time
 UNSEGMENTED ALL NODES
PARTITION BY (log_date) ;


CREATE TABLE public.CDR_LOGS_LOADER
(
    log_time timestamp DEFAULT current_timestamp::timestamptz,
    log_application varchar(256) NOT NULL,
    loaded_rows int,
    rejected_rows int,
    duration_sec int,
    log_date date NOT NULL DEFAULT date_trunc('DAY', current_timestamp)::date
) order by log_time
 UNSEGMENTED ALL NODES
PARTITION BY (log_date) ;

CREATE TABLE public.CDR_LOGS_AGG
(
    log_time timestamp DEFAULT current_timestamp::timestamptz,
    log_application varchar(60),
    start_hour timestamp,
    rows_inserted int,
    duration_sec int,
    log_date date NOT NULL DEFAULT date_trunc('DAY', current_timestamp)::date
) order by log_time
 UNSEGMENTED ALL NODES
PARTITION BY (log_date) ;