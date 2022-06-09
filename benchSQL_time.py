import sqlite3
import csv
import threading
import random
import time

MAX_THREAD = 300
lock = threading.Lock()

def threadFunction(rows,idx):
    try:
        lock.acquire(True)
        letter = "-------------------Thread number "
        letter += str(idx)
        letter += " is launch !-------------------------whit id "
        letter += str(rows[idx][0])
        letter += "--------------------------\n"
        print(letter)

        if rows[idx][6] == 'insert':
            cur.execute("insert into persons values (?, ?, ?, ?, ?)", (rows[idx][0], rows[idx][1],rows[idx][2],rows[idx][3],rows[idx][4]))
        elif rows[idx][6] == 'delete':
            cur.execute("delete from persons where id =%s" %rows[idx][0])
        elif rows[idx][6] == 'update':
            time.sleep(0.1)
            id = rows[idx][0]
            sql_querie = cur.execute("SELECT * FROM persons WHERE id = '%s'" % id)
            list_temp = rows[idx][:]
            for i in range(len(rows[idx])):
                if rows[idx][i] == 'null' and sql_querie[0][i]:
                    list_temp[i] = str(sql_querie[0][i])
            cur.execute("update persons set first_name = ?, last_name = ?, country = ?,  profession = ? where id = ?",(list_temp[1],list_temp[2],list_temp[3],list_temp[4],list_temp[0]))
            #  (rows[idx][0], rows[idx][1],rows[idx][2],rows[idx][3],rows[idx][4]))
        #con.commit()
    finally:
        lock.release()
#-------------------------Get the Excel--------------------------------
file = open("workload.csv")
csvreader = csv.reader(file)
header = next(csvreader)
rows = []
for row in csvreader:
    rows.append(row)
file.close()

con = sqlite3.connect('storage.db' ,check_same_thread=False)
cur = con.cursor()

#---------------------Create table is its needed------------------------
# cur.execute('''CREATE TABLE persons
#                (id integer, first_name text, last_name text, country text, profession text, PRIMARY KEY(id))''')
#------------------------Preparation-----------------------------------
num_threads = 0
num_tasks = len(rows) #get the num of tasks
count = 1
prev_threads = 0
#------------------Main loop for batchs---------------------------------
while num_tasks > 0:
    msg = "#################################### Batch number: "
    msg += str(count)
    msg +=" ####################################"
    print(msg)

    threads = []

    if num_tasks <= MAX_THREAD:
        num_threads = num_tasks
    else:
        num_threads = MAX_THREAD
    #------------------Creation of the threads-----------------------------
    for i in range(num_threads):
        t = threading.Thread(target=threadFunction, args=(rows,i+prev_threads))
        threads.append(t)

    #---------------------------------initiation---------------------------
    for x in threads:
        x.start()

    #--------------------------Wait for all of them to finish----------------
    for x in threads:
        x.join()
    #------Uptdate the actual number of tasks ramaining to do for the next batch----------
    num_tasks = num_tasks - num_threads
    prev_threads += num_threads
    count += 1


# Insert a row of data
#cur.execute("INSERT INTO persons VALUES ('1','BUY','RHAT','israel','35.14')")

# Save (commit) the changes
con.commit()

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
con.close()