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
import configHDR
import gzip



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
    values ('%s','%s' , '%s' , %d , %d) """ % (log_time, log_application, loaded_rows,rejected_rows,duration_sec)
    cursor.execute(select_log)
    cursor.connection.commit()


def sql_log_files(cursor, log_time, loaded_rows, rejected_rows, duration_sec, path_shorter, error_msg='',):
    log_application = error_msg.replace('\\n', ' ').replace("'", '')
    print  ( 'path - %s ; dur - %s ' % (path_shorter,duration_sec) )
    pl = path_shorter.split('_', path_shorter.count('_') )
    if(rejected_rows > 0):
        success = False
    else:
        success = True

    select_log="""insert into public.CDR_LOGS_LOADER_FILES (file_name,unit_name,file_ip,mediation_ip,seq,file_date, data_type, version
                , success
                ,error_msg,loaded_rows, rejected_rows, duration_sec)
    values ('%s','%s' , '%s' ,'%s','%s' , '%s' ,  '%s' ,'%s','%s' ,'%s' ,  %d , %d , %d ) """ % (path_shorter + '.csv.gz'
                ,pl[0], pl[1], pl[2], pl[3], pl[4], pl[5], pl[6]
                , success
                , error_msg,loaded_rows,rejected_rows,duration_sec)
    cursor.execute(select_log)
    cursor.connection.commit()


def loader(configLoad):

    # task parameters
    data_dir = configLoad['data_dir']
    exceptions_path = configLoad['exceptions']
    rejected_path = configLoad['rejected']
    pattern = configLoad['pattern']
    connection_options = dict(
        dsn='vertica',
        user='dbadmin',
        password='dbadmin',
        label='loader_%s' % str(time.time()).replace('.', '_')
    )

    copy_sql = configLoad['copyCMD']


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
    os.environ['VERTICAINI'] = '/opt/vertica/config/vertica.ini'
    global connection
    connection = pyodbc.connect(**connection_options)
    if set(all_files) ^ set(filtered):
        logging.warning('CHECK PREVIOUS EXECUTION!!!')
        logging.warning('should not be any other files except files to copy')
    if not filtered:
        logging.info('DONE: no files to process for the pattern %s ' % pattern)
        sql_log(connection.cursor(), startfmt, 0, 0, 0, 'no files to process for the pattern %s ' % pattern)
        connection.close()
        return

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
        start_loader_file=datetime.datetime.now()
        copy_file = os.path.join(work_dir, path)
        path_short = path.replace('.gz', '')
        exception_file = exceptions_path %path_short
        rejected_file = rejected_path %path_short
        cursor = connection.cursor()
        cursor.execute(copy_sql % (copy_file, exception_file, rejected_file))
        cursor.execute(
            'select get_num_accepted_rows(), get_num_rejected_rows()')
        accepted, rejected = cursor.fetchone()
        stats['accepted'] += accepted
        stats['rejected'] += rejected
        stats['total'] += (accepted + rejected)
        logging.info('COPY DONE. File: %r, accepted: %d, rejected: %d',
                     path, accepted, rejected)
        path_shorter = path_short.replace('.csv', '')
        dur = datetime.datetime.now() - start_loader_file
        total_seconds = dur.days * 24 * 60 * 60 + dur.seconds
        sql_log_files(connection.cursor(), startfmt, accepted, rejected, round(total_seconds, 1),path_shorter, '')
        if rejected > 0:
            with open(rejected_file) as src, gzip.open(rejected_file+'.gz', 'wb') as dst:
                dst.writelines(src)
                os.remove(rejected_file)
            #gzip.GzipFile(filename=rejected_file)

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
    sql_log(cursor, startfmt, stats['accepted'], stats['rejected'], round(total_seconds, 1), 'Load files %s' %pattern)
    connection.close()


def main(log_file=None):
# LOAD CDRs
    loader(config.copyCDRV7);
# LOAD HDRs
    loader(configHDR.copyHDRV7);


if __name__ == '__main__':
    log_file = config.loader_log
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
        #sql_log(connection.cursor(), startfmt, 0, 0, 0, 'Error: %s' % err50)
        #connection.close()
    except Exception, err:
        logging.exception(err)
        err50 = err.message.replace('\\n', ' ').replace("'", '')[0:230]
        #sql_log(connection.cursor(), startfmt, 0, 0, 0, 'Error: %s' % err50)
        #connection.close()

