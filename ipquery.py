#!/usr/bin/env /usr/local/bin/python2.7

import argparse
import datetime
import socket
import sys
import config
import pyodbc
import csv
import os


def is_valid_ipv4_address(address):
    try:
        socket.inet_pton(socket.AF_INET, address)
    except AttributeError:  # no inet_pton here, sorry
        try:
            socket.inet_aton(address)
        except socket.error:
            return False
        return address.count('.') == 3
    except socket.error:  # not a valid address
        return False

    return True

def is_valid_ipv6_address(address):
    try:
        socket.inet_pton(socket.AF_INET6, address)
    except socket.error:  # not a valid address
        return False
    return True
    
def validate_date(d):
    try:
        datetime.datetime.strptime(d , "%Y-%m-%d %H:%M" ) 
        return True
    except ValueError:
        return False
def agr_validation():
# example: ./ipquery.py '120.20.20.1' '2014-05-13 16:00' '2014-05-13 17:00'
    parser = argparse.ArgumentParser('ipquery')
    parser.add_argument('sourceIP', help="sourceIP")
    parser.add_argument('startTime', help="start hour like this: '2014-09-05 11:00'")
    parser.add_argument('endTime', help="start hour like this: '2014-09-05 14:00'")
    args = parser.parse_args()
    print(args)
    if not validate_date(args.startTime) :
        print("Not a valid startTime date - it should be like '2014-05-13 11:00'")
        sys.exit(-1)
    if not validate_date(args.endTime) :
        print("Not a valid endTime date - it should be like '2014-05-14 14:00'")
        sys.exit(-1)
    if not is_valid_ipv4_address(args.sourceIP) :
        print("sourceIP not a valid ipv4 address"  )
        sys.exit(-1) 
    return args
        
def main():
    args=agr_validation()
#   Convert to CSV

# export to csv
    csv_file_path = config.ipquery_csv_file_path
    csv_file_prefix = config.csv_file_prefix
    date2find = datetime.datetime.strptime(args.startTime, "%Y-%m-%d %H:%M" )
    date2find_end = datetime.datetime.strptime(args.endTime, "%Y-%m-%d %H:%M" )
    date_suff = '%s_%s' % ( date2find.strftime("%d.%m_%H_%M"),date2find_end.strftime("%d.%m_%H_%M"))
    csv_suff = "%s_%s" % (args.sourceIP, date_suff)
    csv_suff = "%s" % args.sourceIP
    endTimeDate = datetime.datetime.strptime(args.endTime, "%Y-%m-%d %H:%M" )

    try:
        file_name= '%s%s_%s.csv' % (csv_file_path,csv_file_prefix,csv_suff)
        with open('%s%s_%s.csv' % (csv_file_path,csv_file_prefix,csv_suff) , 'w') as csv_file:
            csv_writer = csv.writer(csv_file, quotechar='"', delimiter=',')
            try:
                
                os.environ['VERTICAINI'] = '/opt/vertica/config/vertica.ini'
                conn = pyodbc.connect(DSN="vertica", user='dbadmin', password='dbadmin', autocommit=False)
                cursor = conn.cursor()
                # IP_source ,IP_dest,Application  ,Start_hour,Bytes_inout
                limit_rows = 1000001
                select_csv = """select inet_ntoa(CDR.IP_source) as IP_source,inet_ntoa(CDR.IP_dest) as IP_dest
                                ,APP.Application, '%s' as Start_hour , '%s' as End_hour
                ,sum(CDR.Bytes_in) as Bytes_in, sum(CDR.Bytes_out) as Bytes_out
                from data.CDR_AGG CDR, data.CDR_Application APP
                where  CDR.IP_source= inet_aton('%s') 
                and Start_hour >= '%s' and Start_hour <= '%s'
                and CDR.Application_id = APP.Application_id
                group by CDR.IP_source , CDR.IP_dest , APP.Application limit %s; 
                """ % (args.startTime,args.endTime,args.sourceIP,args.startTime,args.endTime,limit_rows) 
                #print select_csv
                cursor.execute(select_csv)
                rows=cursor.rowcount
                if rows ==  limit_rows:
                    print ( 'The query returned more the 1 million rows')
                    print ( 'The output csv file will contain the 1 million rows')
                    print ( 'You can narrow the time window for getting all rows')
            except pyodbc.Error, err:
                print err
            csv_writer.writerow(['IP_source','IP_dest','Application','Start_hour','End_hour','Bytes_in','Bytes_out'])
            csv_writer.writerows(cursor)
            
            print( 'Exported to csv file: %s ' %  file_name)
    except IOError as ioer:
            print(ioer)
    
if __name__ == '__main__':
    try:
        main()
    except Exception, err:
        print(err.message, err.args)






