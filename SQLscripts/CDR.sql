create schema data;

CREATE TABLE data.CDR
(
    Start_time timestamp NOT NULL,
    IP_source int,
    IP_dest int,
    Application varchar(128),
    Bytes_in int,
    Bytes_out int,
    Start_hour timestamp NOT NULL DEFAULT date_trunc('HOUR', CDR.Start_time)
)
PARTITION BY (CDR.Start_hour);


CREATE PROJECTION data.CDR_super
(
 Start_time ENCODING RLE,
 IP_source ENCODING DELTARANGE_COMP,
 IP_dest ENCODING DELTARANGE_COMP,
 Application ENCODING RLE,
 Bytes_in ENCODING DELTAVAL,
 Bytes_out ENCODING DELTAVAL,
 Start_hour ENCODING RLE
)
AS
 SELECT CDR.Start_time,
        CDR.IP_source,
        CDR.IP_dest,
        CDR.Application,
        CDR.Bytes_in,
        CDR.Bytes_out,
        CDR.Start_hour
 FROM data.CDR
 ORDER BY CDR.Application,
          CDR.Start_hour,
          CDR.Start_time,
          CDR.IP_source,
          CDR.IP_dest
SEGMENTED BY hash(CDR.IP_source , CDR.IP_dest ) ALL NODES KSAFE 1;

SELECT MARK_DESIGN_KSAFE(1);


