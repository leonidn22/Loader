#!/usr/bin/env /usr/local/bin/python2.7


import pyodbc
import os
import datetime
import config,logger
import configHDR


# todo: double check
log = logger.log

# 


def sql_log(cursor,log_time,start_hour,rows_inserted,duration_sec, msg):
#    msg = msg.replace('\\n', ' ').replace("'", '')
    #print ('print=%s' % msg ) 
    select_log="""insert into public.CDR_LOGS_AGG (log_time, log_application , start_hour ,rows_inserted , duration_sec)
    values ('%s', '%s' , '%s' , %d , %d) """ % (log_time,msg,start_hour,rows_inserted,duration_sec)
    cursor.execute(select_log)
    cursor.connection.commit()


def agg_process(raw_type,config_param):

    log.info('START CDR Aggregation')
    now=datetime.datetime.now()
# For eliminate contention with copy command drop previously aggregated partition.
    delta = datetime.timedelta(hours=4)
    start_hour=(now-delta).strftime("%Y-%m-%d %H:00:00")
    select_part = """SELECT DROP_PARTITION('data.CDR', '%s') """ % start_hour
    try:
        cursor.execute(select_part)
        row = cursor.fetchall()
        msg='Dropping partition for data.CDR by %s: %s' % (start_hour , row)
        log.info( msg )
    except pyodbc.Error as e:
        msg = 'Exception: %s '% e
        log.error( msg )


    delta = datetime.timedelta(hours=3)
    start_hour=(now-delta).strftime("%Y-%m-%d %H:00:00")
# check if the script was running at the same hour
    cursor.execute("""select count(*) as countagg from data.%s_AGG where Start_hour = '%s' limit 1;""" % (raw_type, start_hour))
    row = cursor.fetchall()
    if(row[0].countagg  > 0):
        print("Agggegation script was already running for Start hour %s" % start_hour)
        #sys.exit(-1)

    Start_agg=datetime.datetime.now()
    startfmt=now.strftime("%Y-%m-%d %H:%M:%S")
    sqls = config_param.aggregation
    for k, v in sorted(sqls.iteritems()):
        print k
        log.info('  .... %r', k)
        sql = sqls[k] % '2015-01-08 14:00:00'
    #logging.info(sql)
        cursor.execute(sql)
        conn.commit()
        inserted = cursor.rowcount
        log.info(' .... rows %r', inserted )
    End_agg=datetime.datetime.now()
    dur = End_agg - Start_agg
    total_seconds = dur.days * 24 * 60 * 60 + dur.seconds
    #Diff_agg = (End_agg - Start_agg).total_seconds()
    Diff_agg = total_seconds
# Log messages into log file and table
    msg='End of %s Agggegation into %s_AGG for %s, inserted %d rows - duration=%s sec'% (raw_type,raw_type,start_hour,inserted,round(Diff_agg,1))
    sql_log(cursor, startfmt, start_hour, inserted, round(Diff_agg,1), 'Inserted into %s_AGG' %raw_type)
    log.info(msg)
    print msg

if __name__ == '__main__':
    try:
        os.environ['VERTICAINI'] = '/opt/vertica/config/vertica.ini'
        log.info( 'VERTICAINI=%s ; user=%s' % (os.environ['VERTICAINI'],os.environ['USER']) )
        conn = pyodbc.connect(DSN='vertica', autocommit=False)
        cursor = conn.cursor()
        sid = conn.execute('select current_session()').fetchone()[0]
    except pyodbc.Error as e:
        msg = 'Exception: %s '% e
        log.error( msg )


    # Start Aggregation
        msg = '--------- Start Agggegation into CDR_AGG [session_id: %s]---------' % sid
        log.info(msg)
    try:
# !! process
        agg_process('CDR', config)
        agg_process('HDR', configHDR)

    except pyodbc.Error as e:
            msg = 'Exception: %s '% e
            log.error( msg )
            #sql_log(cursor,msg,'Error')



