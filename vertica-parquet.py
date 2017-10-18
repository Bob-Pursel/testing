import sys
import vertica_python

dirname = sys.argv[1]
tablename = sys.argv[2]

conn_info = {'host':'172.25.4.115', 'port':5433, 'user':'dbadmin','password':'dbadmin','database':'dwp1','read_timeout':600,'unicode_error':'strict','ssl':False,'connection_timeout':10}

connection = vertica_python.connect(**conn_info)

cur = connection.cursor()

cur.execute("EXPORT TO PARQUET(directory='{0}') AS SELECT * FROM {1}".format(dirname, tablename))
connection.commit()    
connection.close()
