import vertica_python
import sys
import os
import datetime
import logger
import logger.config

logging.config.fileConfig('logging.conf')
logger = logging.getLogger("testApp")

# Vertica connection
def createVeticaConnection():
    conn_info = {'host':'172.25.4.115', 'port':5433, 'user':'dbadmin','password':'dbadmin','database':'dwp1','read_timeout':600,'unicode_error':'strict','ssl':False,'connection_timeout':10}
    connection = vertica_python.connect(**conn_info)
    return connection

# Get list of directories to search for new files
def readSearchList():
    conn = createVerticaConnection()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT config_num, search_path FROM etlConfig")
    searchPaths = cur.fetchall()
    conn.close()
    return searchPaths
  
# Get File Num
def getFileNum(configNum, fileName):
    conn = createVerticaConnection()
    cur = conn.cursor()
    cur.execute("SELECT file_num FROM fileConfig WHERE config_num = {0} and file_name = {1}".format(configNum, fileName))
    for row in cur.iterate():
        fileNum = row[0]
    cur.close()
    return fileNum
 
# Register file
def registerFile(fileNum, configNum, dataPath, fileName):
    conn = createVerticaConnection()
    cur = conn.cursor()
    currentDate = datetime.datetime.now()
    try:
        cur.execute("INSERT INTO file_log (log_num, file_num, config_num, data_path, file_name, receivedDate) VALUES (log_file_seq.nextval, {0}, {1}, {2}, {3}, {4})".format(fileNum, comfigNum, dataPath, fileName, currentDate))
    except:
        print("Error on Register File {0} {1}".format(dataPath, fileName)))
    conn.commit()
    conn.close()

# Check if file is being processed 
def fileReceived(dataPath, fileName):
    conn = createVerticaConnection()
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM file_log WHERE data_path = {0} and file_name = {1} and received_flag = 'Y'".format(dataPath, fileName))
    for row in cur.iterate()
        if row[0] > 0:
            conn.close()
            return 1
        else:
            conn.close()
            return 0

# Main

while True:

    pathways =  readSerachList()

    # get directory search paths
    for row in pathways.iterate():
       cfgNum = row[0]
       path = row[1]
       
       # Find files
       try:
           for f in listdir(path):
               if fileRecieved(path, f) = 0:
                   fn = getFileNumber(cfgNum, f)
                   registerFile(fn, cfgNum, path, f)    
       except FIleNotFoundError:
           pass
       except:
           print("Error on File Search {0}".format(path))
    sleep(1800)  
