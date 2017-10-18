import vertica_python

conn_info = {'host':'172.25.4.115', 'port':5433, 'user':'dbadmin','password':'dbadmin','read_timeout':600,'unicode_error':'strict','ssl':False,'connection_timeout':10}

connection = vertica_python.connect(**conn_info)

cur = connection.cursor()

for row in cur.iterate()
    print(row)
    
connection.close()