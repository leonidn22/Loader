#!/usr/bin/env /usr/local/bin/python2.7
#
# file          : drop_partition.py
# author        : Daniel Leybovich
# change logging to log instead of print : Leonid Neizberg
# description   : Frees space
import os
import sys
import math
import pyodbc
import fnmatch
import logging


SCRIPT_NAME = os.path.basename(sys.argv[0])


def usage():
    print 'USAGE:'
    print '         %s <PATH ON HDD>' % SCRIPT_NAME
    print
    print 'EXAMPLE:'
    print
    print '         $> %s /vertica' % SCRIPT_NAME
    print
    print '  -h/--help        - prints usage'
    print


def disk_usage(path):
    """Report disk space usage for particular path.
    `disk_usage` displays the amount of disk space available for particular
    path on the file system containing each file name argument.

    Disk space is shown in 1K blocks.
    """
    hdd = os.statvfs(path)
    total_size = int(hdd.f_bsize * hdd.f_blocks)
    used_space = int(hdd.f_bsize * (hdd.f_blocks - hdd.f_bfree))
    free_space = int(hdd.f_bsize * hdd.f_bfree)
    return total_size, used_space, free_space

def main(log_file=None):
    # set logger
    if not log_file:
        log_level = logging.DEBUG
        log_format = '%(asctime)s %(levelname)-8s %(message)s'
        logging.basicConfig(level=log_level, format=log_format)

    logging.info('START')

    # validate command line arguments
    if len(sys.argv) != 2 or sys.argv[1] in ('-h', '--help'):
        usage()
        sys.exit(0)

    # check path for free space
    dev_path = sys.argv[1]

    # path is valid?
    if not os.path.exists(dev_path):
        print '\n\033[1;31mERROR: Invalid path \033[0m%r' % dev_path
        usage()
        sys.exit(-1)

    # if everythink is ok, so continue with process
    total, used, free = disk_usage(dev_path)
    percent_usage = int(math.ceil(100.0 * used / total))
    logging.info('Path:%s' % dev_path)
    logging.info('Total device size: %d bytes' % total)
    logging.info('Used space       : %d bytes' % used)
    logging.info('Free space       : %d bytes' % free)
    #logging.info('-' * 66
    logging.info('DISK USE : %d%%' % percent_usage )

    if percent_usage > 70:
        logging.info('Disk usage is more than 70 percent => drop partition')
        connection = pyodbc.connect(dsn='vertica')
        cursor = connection.execute("select date_trunc('DAY', min(Start_hour)) from public.CDR_AGG")
        min_partition = cursor.fetchone()[0]
        if min_partition:
            min_partition = min_partition.strftime('%Y-%m-%d %T')
        else:
            logging.info('NO DATA, DONE => EXIT')
            sys.exit(0)
        query = "select drop_partition('data.CDR_AGG', %r)" % min_partition
        logging.info('MIN PARTITION IS: %s' % min_partition)
        logging.info('QUERY IS: %s' % query)
        cursor = connection.execute(query)
        dropped = cursor.fetchone()[0]
        logging.info('%s ' % dropped)
    else:
        logging.info('Disk usage is OK!')

if __name__ == '__main__':

    log_file = '/opt/allot/vftrk/logs/drop_partition.log'
    log_format = '%(asctime)s %(levelname)-8s %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=log_format, filename=log_file, filemode='a')
    try:
        main()
    except Exception, err:
        logging.exception(err)



