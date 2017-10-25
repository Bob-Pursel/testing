import sys
import os
import vertica_python
import shutil
import logging
import logging.config
#import subprocess

dirname = sys.argv[1]
tablename = sys.argv[2]
parquet_table = sys.argv[3]
stagename = dirname + '-stage'

logging.config.fileConfig('logging.conf')
logger = logging.getLogger("testApp")

logger.info("Start export to parquet {0}".format(parquet_table))

# Define connection to Vertica
conn_info = {'host':'172.25.4.115', 'port':5433, 'user':'dbadmin','password':'dbadmin','database':'dwp1','read_timeout':600,'unicode_error':'strict','ssl':False,'connection_timeout':10}

connection = vertica_python.connect(**conn_info)

cur = connection.cursor()

#try:
#    rm_dir = "if [ -d {0} ]; then rm -rf {0}; fi".format(dirname)
#    subprocess.Popen(rm_dir, shell=True)
#except:
#    print("Error on Parquet Directory Removal {0}".format(dirname))
#    raise

# Remove parquet stage directory and any files if they exist
try:
    if os.stat(stagename):
        shutil.rmtree(stagename)
except FileNotFoundError:
    pass
except:
    logger.error("Error Removing Parquet Stage Directory {0}".format(stagedir), exc_info=True)
    raise

# Write parquet files in stage directory from Vertica table
try:
    cur.execute("EXPORT TO PARQUET(directory='{0}') AS SELECT * FROM {1}".format(stagename, tablename), exc_info=True))
except:
    logger.error("Error on Export to Parquet {0} {1}".format(stagename, tablename), exc_info=True))
    raise

#try:
#    columns = '(period varchar, formularyId int, medId int, imsPayerPlanId varchar, ndc varchar, ddi int)'
#    cur.execute("CREATE EXTERNAL TABLE {0} {1} AS COPY FROM '{2}' PARQUET".format(parquet_table, columns, dirname))
#except:
#    print("Error on External Table Create {0} {1}".format(dirname, parquet_table))
#    raise

# Move parquet files from stage to final directory
try:
    for name in os.listdir(stagename):
        src_file = os.path.join(stagename, name)
        dst_file = os.path.join(dirname, name)
        shutil.move(src_file, dst_file)
except:
    logger.error("Error when moving parquet file {0} {1}".format(stagename, dirname), exc_info=True))
    raise

# Truncate native Vertica table data
try:
    cur.execute("TRUNCATE TABLE {0}".format(tablename))
except:
    logger.error("Error on Truncate Table {0}".format(tablename), exc_info=True))
    raise

# Run Analyze for external table to see new parquet files
try:
    cur.execute("SELECT ANALYZE_STATISTICS ('{0}')".format(parquet_table))
except:
    logger.error("Error on Anylyze Table {0}".format(parquet_table), exc_info=True))
    raise

# Cleanup
connection.commit()
connection.close()

logger.info("End export to parquet {0}".format(parquet_table))
