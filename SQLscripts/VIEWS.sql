CREATE  VIEW public.CDR AS
 SELECT CDR.Start_time,
        inet_ntoa(CDR.IP_source) AS IP_source,
        inet_ntoa(CDR.IP_dest) AS IP_dest,
        CDR.Application,
        CDR.Bytes_in,
        CDR.Bytes_out,
        (CDR.Bytes_in + CDR.Bytes_out) AS Bytes_Total,
        CDR.Start_hour
 FROM data.CDR;


CREATE  VIEW public.CDR_AGG AS
 SELECT inet_ntoa(CDR_AGG.IP_source) AS IP_source,
        inet_ntoa(CDR_AGG.IP_dest) AS IP_dest,
        APP.Application,
        CDR_AGG.Start_hour,
        CDR_AGG.Bytes_in,
        CDR_AGG.Bytes_out
 FROM data.CDR_AGG, data.CDR_Application APP
 WHERE (CDR_AGG.Application_id = APP.Application_id);

SELECT MARK_DESIGN_KSAFE(1);

