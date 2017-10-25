import sys
import os
import shutil
import logging
import logging.config
import vertica_python

pathname = sys.argv[1]
filename = sys.argv[2]
tablename = sys.argv[3]
delimiter = sys.argv[4]
fullfilename = pathname + filename

logging.config.fileConfig('test.conf')
logger = logging.getLogger("testApp")

logger.info("Start load Vertica table {0}".format(tablename))

conn_info = {'host':'172.25.4.115', 'port':5433, 'user':'dbadmin','password':'dbadmin','database':'dwp1','read_timeout':600,'unicode_error':'strict','ssl':False,'connection_timeout':10}

connection = vertica_python.connect(**conn_info)

cur = connection.cursor()

# Copy data to Vertica
try:
    with open(fullfilename,"rb") as csv:
        my_file = csv.read().decode('utf-8')
        cur.copy("Copy {0} FROM stdin PARSER fcsvparser( type='traditional', delimiter='{3}')".format(tablename), my_file, delimiter)
except:
    logger.error("Error on copy to table {0}".format(tablename), exc_info=True)
    raise

connection.commit()    
connection.close()

# Move file to processed subdirectory

try:
    dst_file = pathname + 'processed/' + filename
    shutil.move(fullfilename, dst_file)
except:
    logger.error("Error on processed file move {0)".format(fullfilename))
    raise

logger.info("End loading Vertica table {0}".format(tablename))
