import sys
import os
import vertica_python
import shutil
import logging
import logging.config
#import subprocess


logger.info("Start export to parquet {0}".format(parquet_table))




def verticaToParquet(tablename, dirname, parquet_table):


    stagename = dirname + '-stage'
    # Remove parquet stage directory and any files if they exist
    try:
        if os.stat(stagename):
            shutil.rmtree(stagename)
    except FileNotFoundError:
        pass
    except:
        logger.error("Error Removing Parquet Stage Directory {0}".format(stagedir), exc_info=True)
        raise

    conn = vertica_python.connect(**conn_info)

    cur = conn.cursor()
    # Write parquet files in stage directory from Vertica table
    try:
        cur.execute("EXPORT TO PARQUET(directory='{0}') AS SELECT * FROM {1}".format(stagename, tablename), exc_info=True))
    except:
        logger.error("Error on Export to Parquet {0} {1}".format(stagename, tablename), exc_info=True))
        raise

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
    return 200

if __name__ == "__main__":
    pathName = sys.argv[1]
    tableName = sys.argv[2]
    parquetTable = sys.argv[3]
    
    logging.config.fileConfig('logging.conf')
    logger = logging.getLogger("testApp")
    
    verticaToParquet(tableName, pathName, parquetTable)

    logger.info("End export to parquet {0}".format(parquet_table))
