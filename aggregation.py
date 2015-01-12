#!/usr/bin/env /usr/local/bin/python2.7


import pyodbc
import os
import datetime
import config,logger


# todo: double check
log = logger.log

# 


def sql_log(cursor,log_time,start_hour,rows_inserted,duration_sec):
#	msg = msg.replace('\\n', ' ').replace("'", '')
	#print ('print=%s' % msg ) 
	select_log="""insert into public.CDR_LOGS_AGG (log_time, log_application , start_hour ,rows_inserted , duration_sec)
	values ('%s','Inserted into CDR_AGG' , '%s' , %d , %d) """ % (log_time,start_hour,rows_inserted,duration_sec)
	cursor.execute(select_log)
	cursor.connection.commit()

if __name__ == '__main__':
	try:
		os.environ['VERTICAINI'] = '/opt/vertica/config/vertica.ini'
		log.info( 'VERTICAINI=%s ; user=%s' % (os.environ['VERTICAINI'],os.environ['USER']) )
		conn = pyodbc.connect(DSN='vertica', autocommit=False)
		cursor = conn.cursor()
		sid = conn.execute('select current_session()').fetchone()[0]


	# Start Aggregation
		msg = '--------- Start Agggegation into CDR_AGG [session_id: %s]---------' % sid
		log.info(msg)
		now=datetime.datetime.now()
		Start_agg=datetime.datetime.now()
		startfmt=now.strftime("%Y-%m-%d %H:%M:%S")

			#start_hour = '2014-05-13 10:00:00'
# For eliminate contention with copy command drop previously aggregated partition. 
		delta = datetime.timedelta(hours=4)
		start_hour=(now-delta).strftime("%Y-%m-%d %H:00:00")
		select_part = """SELECT DROP_PARTITION('data.CDR', '%s') """ % start_hour
		cursor.execute(select_part)
		row = cursor.fetchall()
		msg='Dropping partition for data.CDR by %s: %s' % (start_hour , row)
		log.info( msg ) 
	except pyodbc.Error as e:
			msg = 'Exception: %s '% e
			log.error( msg )

	try:
		delta = datetime.timedelta(hours=3)
		start_hour=(now-delta).strftime("%Y-%m-%d %H:00:00")
	# check if the script was running at the same hour
		cursor.execute("""select count(*) as countagg from data.CDR_AGG where Start_hour = '%s' limit 1;""" % start_hour )
		row = cursor.fetchall()
		if(row[0].countagg  > 0):
			print("Agggegation script was already running for Start hour %s" % start_hour)
			#sys.exit(-1)

		#sql_log(cursor,msg)
		select_dim = """
		insert /*+direct*/ into data.CDR_Application (Application)
		select distinct(Application) from data.CDR as CD
		where Start_hour = '%s' and Application is not null
		  and not exists( select 1 from data.CDR_Application CA where CA.Application = CD.Application) ;
		"""% start_hour
		cursor.execute(select_dim)
		conn.commit()
		
		select_csv="""insert /*+direct*/ into data.CDR_AGG 
		select CDR.IP_source,CDR.IP_dest,APP.Application_id, '%s' as Start_hour
		,sum(Bytes_in), sum(Bytes_out) from data.CDR CDR, data.CDR_Application APP
		where Start_hour = '%s'  
		and CDR.Application = APP.Application
		and CDR.IP_source is not null and CDR.IP_dest is not null
		group by CDR.IP_source , CDR.IP_dest , APP.Application_id ;"""% (start_hour,start_hour)
		#select_csv="""select * from aall_tables;"""
		cursor.execute(select_csv)
		inserted = cursor.rowcount
		conn.commit()
		End_agg=datetime.datetime.now()
		dur = End_agg - Start_agg
		total_seconds = dur.days * 24 * 60 * 60 + dur.seconds
		#Diff_agg = (End_agg - Start_agg).total_seconds()
		Diff_agg = total_seconds
	# Log messages into log file and table
		msg='End of Agggegation into CDR_AGG for %s, inserted %d rows - duration=%s sec'% (start_hour,inserted,round(Diff_agg,1))
		sql_log(cursor,startfmt,start_hour,inserted,round(Diff_agg,1))
		log.info( msg )
	except pyodbc.Error as e:
			msg = 'Exception: %s '% e
			log.error( msg )
			#sql_log(cursor,msg,'Error')



