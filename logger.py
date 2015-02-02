#!/usr/bin/env /usr/local/bin/python2.7
# 
import os
import logging
import datetime,time
import config

#### Variables declaration
logfile = os.path.join(config.root_path, 'logs/aggregation.log')
logname = 'Aggregate process'
logformat = '%(asctime)s %(levelname)-8s    %(message)s'


#############################################################################
# create logger with 'Aggregate process'
log = logging.getLogger(logname)
log.setLevel(logging.DEBUG)

# create file handler which logs even debug messages
fh = logging.FileHandler(logfile)
fh.setLevel(logging.DEBUG)

# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)

# create formatter and add it to the handlers
formatter = logging.Formatter(logformat)
fh.setFormatter(formatter)
ch.setFormatter(formatter)

# add the handlers to the logger
log.addHandler(fh)
log.addHandler(ch)

