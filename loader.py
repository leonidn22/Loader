#!/usr/bin/env /usr/local/bin/python2.7
#
# file      : loader.py
# notes:
#   * load all files of raw data every 5 min
#   * loaded data files shell be deleted
#   * write loading info and errors to a log file
#   * application size shall be reduced to the minimum possible
import os
import sys
import time
import datetime
import pyodbc
import shutil
import fnmatch
import logging
import config



# create global vars
cursor = pyodbc.Cursor
connection = pyodbc.Connection
startfmt=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class LoaderException(Exception):
    pass

def sql_log(cursor, log_time, loaded_rows, rejected_rows, duration_sec, log_application='Load into data.CDR'):
    log_application = log_application.replace('\\n', ' ').replace("'", '')
#print ('print=%s' % msg )
    select_log="""insert into public.CDR_LOGS_LOADER (log_time,log_application,loaded_rows, rejected_rows, duration_sec)
    values ('%s','%s' , '%s' , %d , %d) """ % (log_time, log_application,loaded_rows,rejected_rows,duration_sec)
    cursor.execute(select_log)
    cursor.connection.commit()


def main(log_file=None):
# task parameters
    data_dir = config.data_dir
    exceptions = config.exceptions
    rejected = config.rejected
    pattern = '*CONV*.csv.gz'
    connection_options = dict(
        dsn='vertica',
        user='dbadmin',
        password='dbadmin',
        label='loader_%s' % str(time.time()).replace('.', '_')
    )

    copy_sql = """copy data.CDR(f filler varchar,Start_time, End_time filler timestamp, f1 filler varchar, IP_Source as INET_ATON(regexp_substr(f1, '(^.+:)?(\d|\.)+', 2))
    , f2 filler varchar, IP_Dest as INET_ATON(regexp_substr(f2, '(^.+:)?(\d|\.)+', 2)), Application
    ,f3 filler varchar,f4 filler varchar,f5 filler varchar,f6 filler varchar,f7 filler varchar,f8 filler varchar,f9 filler varchar,f10 filler varchar,f11 filler varchar
    ,f12 filler varchar,f13 filler varchar,f14 filler varchar,f15 filler varchar,f16 filler varchar,f17 filler varchar,f18 filler varchar,f19 filler varchar,f20 filler varchar
    , Bytes_in, Bytes_out) 
    from '%s' gzip direct delimiter ',' enclosed '"' skip 1
    EXCEPTIONS '%s' REJECTED DATA '%s' ;
    """

    # set logger
    if not log_file:
        log_level = logging.DEBUG
        log_format = '%(asctime)s %(levelname)-8s %(message)s'
        logging.basicConfig(level=log_level, format=log_format)

    logging.info('START')

    # get files to copy
    all_files = os.listdir(data_dir)
    filtered = fnmatch.filter(all_files, pattern)
    start_loader=datetime.datetime.now()
    global connection
    connection = pyodbc.connect(**connection_options)
    if set(all_files) ^ set(filtered):
        logging.warning('CHECK PREVIOUS EXECUTION!!!')
        logging.warning('should not be any other files except files to copy')
    if not filtered:
        logging.info('DONE: no files to process')
        sql_log(connection.cursor(), startfmt, 0, 0, 0, 'no files to process')
        connection.close()
        sys.exit(0)

    sid = connection.execute('select current_session()').fetchone()[0]
    logging.debug('  .... created new connection [session_id: %s]', sid)
    # create temporary working directory
    work_dir = os.path.join(data_dir, connection_options['label'])
    os.mkdir(work_dir)
    logging.debug('  .... created work_dir %r' % work_dir)

    # hide files in temporary working directory
    for path in filtered:
        copy_file = os.path.join(data_dir, path)
        shutil.move(copy_file, work_dir)
        logging.debug('  .... file %r moved to work_dir', path)

    stats = dict(accepted=0, rejected=0, total=0)
    # copy files from working directory
    for path in filtered:
        copy_file = os.path.join(work_dir, path)
        cursor = connection.cursor()
        cursor.execute(copy_sql % (copy_file, exceptions, rejected))
        cursor.execute(
            'select get_num_accepted_rows(), get_num_rejected_rows()')
        accepted, rejected = cursor.fetchone()
        stats['accepted'] += accepted
        stats['rejected'] += rejected
        stats['total'] += (accepted + rejected)
        logging.info('COPY DONE. File: %r, accepted: %d, rejected: %d',
                     path, accepted, rejected)
        #shutil.move(copy_file, '/opt/allot/vftrk/testdata')
        os.remove(copy_file)
        logging.info('FILE %r DELETED', copy_file)

    # clean
    os.rmdir(work_dir)
    logging.info('work_dir %r DELETED', work_dir)
    logging.info('EXIT. Total: %d, loaded: %d, rejected: %d',
                 stats['total'], stats['accepted'], stats['rejected'])
    end_loader=datetime.datetime.now()
    dur = end_loader - start_loader
    total_seconds = dur.days * 24 * 60 * 60 + dur.seconds
    sql_log(cursor, startfmt, stats['accepted'], stats['rejected'], round(total_seconds, 1))
    connection.close()

if __name__ == '__main__':
    log_file = '/opt/allot/vftrk/logs/loader.log'
    log_format = '%(asctime)s %(levelname)-8s %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=log_format, filename=log_file, filemode='a')
    try:
        main()
    except pyodbc.Error, e:
        logging.exception(e)
        if(len(e.args) > 1):
            msg=e.args[1]
        else:
            msg=e.args[0]
        err50 = msg.replace('\\n', ' ').replace("'", '')[0:230]
        sql_log(connection.cursor(), startfmt, 0, 0, 0, 'Error: %s' % err50)
        connection.close()
    except Exception, err:
        logging.exception(err)
        err50 = err.message.replace('\\n', ' ').replace("'", '')[0:230]
        sql_log(connection.cursor(), startfmt, 0, 0, 0, 'Error: %s' % err50)
        connection.close()

