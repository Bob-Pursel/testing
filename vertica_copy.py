import sys
import os
import shutil
import logging
import logging.config
import vertica_python
import common_functions


def copyFileToVertica(pathname, filename, tablename, delimiter):
    
    logger.info("Start load Vertica table {0}".format(tablename))

    connection = getVerticaConnection()

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
    return 200
    
if __name__ == "__main__":
    pathName = sys.argv[1]
    fileName = sys.argv[2]
    tableName = sys.argv[3]
    Delimiter = sys.argv[4]
    
    logging.config.fileConfig('test.conf')
    logger = logging.getLogger("testApp")
    
    copyFileToVertica(pathName, fileName, tableName, Delimiter)
