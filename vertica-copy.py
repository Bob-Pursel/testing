import sys
import vertica_python

filename = sys.argv[1]
tablename = sys.argv[2]

conn_info = {'host':'172.25.4.115', 'port':5433, 'user':'dbadmin','password':'dbadmin','database':'dwp1','read_timeout':600,'unicode_error':'strict','ssl':False,'connection_timeout':10}

connection = vertica_python.connect(**conn_info)

cur = connection.cursor()

with open(filename,"rb") as csv:
    my_file = csv.read().decode('utf-8')
    cur.copy("Copy {0} FROM stdin PARSER fcsvparser( type='traditional', delimiter='|')".format(tablename), my_file)
    connection.commit()    
connection.close()
