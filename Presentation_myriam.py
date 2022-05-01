import csv
from cassandra.cluster import Cluster
from decimal import Decimal
import numpy
import time
import threading
import random

MAX_THREAD = 300

#Function for every Thread in work
def threadFunction(liste,pos):

	#Message from the Thread :
	letter = "Thread number "
	letter += str(pos)
	letter += " is launch !\n"

	print(letter)

	#first identify the action from the ERP server DELETE, UPDATE OR ADD

	if liste[pos][5] == 'add':
		# adding if it allowed by the timestamp;
		session.execute(
		"""INSERT INTO test_table(id,first_name,last_name,country,profession) VALUES (%(id)s, %(first_name)s, %(last_name)s, %(country)s, %(profession)s) USING TIMESTAMP %(time_st)s""",
		{'id' : liste[pos][0], 'first_name' : liste[pos][2], 'last_name' : liste[pos][3], 'country' : liste[pos][1], 'profession' : liste[pos][4], 'time_st' : int(liste[pos][6])}
		)

	elif liste[pos][5] == 'delete':

		# delete if it allowed by the timestamp;
		session.execute(
			"""DELETE FROM test_table USING TIMESTAMP %(time_st)s WHERE id = %(id)s""",
			{'time_st' : int(liste[pos][6]), 'id': liste[pos][0]}
			)

	elif liste[pos][5] == 'update':
		#updating if it allowed by the timestamp;

		time.sleep(3)
		cassandra_temp = session.execute("""SELECT * FROM test_table WHERE id = %(id)s""", {'id': liste[pos][0]})
		#check what there is to update
		list_temp = liste[pos][:]
		for i in range(len(liste[pos])):
			if liste[pos][i] == 'null' and cassandra_temp[0][i]:
				list_temp[i] = str(cassandra_temp[0][i])

		session.execute(
		"""INSERT INTO test_table(id,first_name,last_name,country,profession) VALUES (%(id)s, %(first_name)s, %(last_name)s, %(country)s, %(profession)s) USING TIMESTAMP %(time_st)s""",
		{'id' : list_temp[0], 'first_name' : list_temp[2], 'last_name' : list_temp[3], 'country' : list_temp[1], 'profession' : list_temp[4], 'time_st' : int(list_temp[6])}
		)


			
		#session.execute(
		#	"""UPDATE test_table USING TIMESTAMP %(time_st)s set """
		#	)


#######################################################################################################################################################################################################
#######################################################################################################################################################################################################

#-------------------------Get the Excel--------------------------------
file = open("test.csv")
csvreader = csv.reader(file)
header = next(csvreader)
rows = []
for row in csvreader:
    rows.append(row)
file.close()

cluster = Cluster()

session = cluster.connect('test_keyspace')

#------------------------Creation of the threads------------------------
threads = []
num_thread = 0
if len(rows) <= MAX_THREAD:
	num_thread = len(rows)
else:
	num_thread = MAX_THREAD

#-------------------------------Preparation----------------------------
for i in range(num_thread):
	t = threading.Thread(target=threadFunction, args=(rows,i))
	threads.append(t)

#---------------------------------initiation---------------------------
for x in threads:
    x.start()

#--------------------------Wait for all of them to finish----------------
for x in threads:
    x.join()

print "finished "
#TRUNCATE test_table;

