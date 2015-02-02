CREATE SEQUENCE data.Application_id_seq ;
CREATE SEQUENCE data.ApplicationHDR_id_seq ;

CREATE TABLE data.CDR_Application
(
    Application_id int NOT NULL DEFAULT nextval('data.Application_id_seq'),
    Application varchar(128) NOT NULL
);


CREATE PROJECTION data.CDR_Application_super
(
 Application_id ENCODING COMMONDELTA_COMP,
 Application
)
AS
 SELECT CDR_Application.Application_id,
        CDR_Application.Application
 FROM data.CDR_Application
 ORDER BY CDR_Application.Application
UNSEGMENTED ALL NODES;


SELECT MARK_DESIGN_KSAFE(1);
