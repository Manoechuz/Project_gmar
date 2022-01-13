import csv
from cassandra.cluster import Cluster
from decimal import Decimal
import numpy
import time
import threading
import random

def insertionRow(rows, i, time_sleep, nom):

	if nom == "":
		nom = rows[i][2]
	val = int(time.time() * 1e6)
	value = time_sleep
	print "this is the value :",value,"\n"
	time.sleep(value)
	print"Thread ",i, "came up","\n"
	session.execute(
	"""INSERT INTO test_table(id,first_name,last_name,country,profession,last_modif) VALUES (%(id)s, %(first_name)s, %(last_name)s, %(country)s, %(profession)s, %(last_modif)s)""",
	{'id' : rows[i][0], 'first_name' : rows[i][1], 'last_name' : nom, 'country' : rows[i][3], 'profession' : rows[i][4], 'last_modif' : val}
	)

def deleteRow(rows, i, time_sleep, nom):

	if nom == "":
		nom = rows[i][2]
	val = int(time.time() * 1e6)
	value = time_sleep
	print "this is the value :",value,"\n"
	time.sleep(value)
	print"Thread ",i, "came up","\n"
	session.execute(
	"""DELETE FROM test_table WHERE id= %(id)s IF EXISTS""",{'id': i}
	)

file = open("test.csv")
csvreader = csv.reader(file)
header = next(csvreader)
#print(header)
rows = []
for row in csvreader:
    rows.append(row)
file.close()

cluster = Cluster()

session = cluster.connect('test_keyspace')

session.execute("CREATE TABLE test_table(id text, first_name text, last_name text,country text, profession text, last_modif timestamp, PRIMARY KEY (id))")

threads = []

t = threading.Thread(target=insertionRow, args=(rows,0,9,"Allalouf"))
threads.append(t)
t = threading.Thread(target=insertionRow, args=(rows,0,0,"Jean"))
threads.append(t)

 # Start all threads
for x in threads:
    x.start()

 # Wait for all of them to finish
#for x in threads:
#    x.join()

print "finished"