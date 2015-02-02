

CREATE TABLE data.CDR_AGG
(
    IP_source int NOT NULL,
    IP_dest int NOT NULL,
    Application_id int NOT NULL,
    Start_hour timestamp NOT NULL,
    Bytes_in int,
    Bytes_out int
)
PARTITION BY (date_trunc('MONTH', CDR_AGG.Start_hour));



CREATE PROJECTION data.CDR_AGG_super
(
 IP_source ENCODING DELTARANGE_COMP,
 IP_dest ENCODING DELTARANGE_COMP,
 Application_id ENCODING BLOCKDICT_COMP,
 Start_hour ENCODING RLE,
 Bytes_in ENCODING DELTAVAL,
 Bytes_out ENCODING DELTAVAL
)
AS
 SELECT CDR_AGG.IP_source,
        CDR_AGG.IP_dest,
        CDR_AGG.Application_id,
        CDR_AGG.Start_hour,
        CDR_AGG.Bytes_in,
        CDR_AGG.Bytes_out
 FROM data.CDR_AGG
 ORDER BY CDR_AGG.IP_source,
          CDR_AGG.Start_hour,
          CDR_AGG.IP_dest,
          CDR_AGG.Bytes_out
SEGMENTED BY hash(CDR_AGG.IP_source , CDR_AGG.IP_dest , CDR_AGG.Application_id) ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);

ToDo:

select
                count(distinct partition_key) as part_num , node_name,sum(ros_row_count),
                projection_name, table_schema
                from partitions
                where projection_name = 'CDR_b0'
                group by projection_name, node_name,table_schema
                order by node_name -- count(distinct partition_key) desc

SELECT
            NODE_NAME,
            ros_count,
            projection_name,
            anchor_table_name,
            projection_schema
        FROM
            projection_storage
        ORDER BY
            ros_count DESC;
