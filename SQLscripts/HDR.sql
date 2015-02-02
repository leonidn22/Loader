
CREATE TABLE data.HDR
(
    Start_time timestamp NOT NULL,
    IP_source int,
    IP_dest int,
    Application varchar(128),
    Bytes_in int,
    Bytes_out int,
    Start_hour timestamp NOT NULL DEFAULT date_trunc('HOUR', HDR.Start_time)
)
PARTITION BY (HDR.Start_hour);



CREATE PROJECTION data.HDR_super
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
 SELECT HDR.Start_time,
        HDR.IP_source,
        HDR.IP_dest,
        HDR.Application,
        HDR.Bytes_in,
        HDR.Bytes_out,
        HDR.Start_hour
 FROM data.HDR
 ORDER BY HDR.Start_time,
          HDR.IP_source,
          HDR.IP_dest,
          HDR.Application,
          HDR.Bytes_in,
          HDR.Bytes_out,
          HDR.Start_hour
SEGMENTED BY hash(HDR.IP_source , HDR.IP_dest) ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);
