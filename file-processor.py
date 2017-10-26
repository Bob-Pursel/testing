import os
import sys
import subprocess
import vertica_python
import common_functions
import importlib


if "__name__" = "__main__":
    while True:
        conn =getVerticaConection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM file_config fc join file_registered fr on fc.file_num = fr.file_num where fr.register_num not in (select register_num from file_processed fp")

        for row in cur.iterate():
            register_num = row[0]
            file_num = row[1]
            file_name = row[2]
            path = row[3]
            table_name = row[4]
            dest_path = row[5]
            transform_name = row[6] 
            delimiter = row[7]
            parquetTable = row[8]
            
            module = importlib.import_module(transform_name, package=None)
            module.startTransform(file_name, path, table_name, dest_path, delimiter, parquetTable)
            
        
    
        conn.close()
