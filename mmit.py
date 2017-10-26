import os
import sys
import vertica_python
import common_functions
import logging
import logging.comfig
import vertica_copy
import vertica_parquet

def startTransform(filename, pathname, tablename, destpath, delimiter, parquet_table):
    
    #Preprocess goes here
    
    #Transformations go here
    
    #Copy to Vertica table
    rc = copyFileToVertica(pathname, filename, tablename, delimiter)
    
    #export to Parquet and truncate Vertica table
    if rc == 200 :
        rc2 = verticaToParquet(tablename, pathname, parquet_table)
    if rc2 == 200 :
        return 200
    else:
        return 500
    
    
    
    