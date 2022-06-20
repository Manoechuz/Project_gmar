import sqlite3
import csv
import threading
import random
import time

MAX_THREAD = 100
lock = threading.Lock()

def threadFunction(rows,idx):
    try:
        lock.acquire(True)
        # letter = "-------------------Thread number "
        # letter += str(idx)
        # letter += " is launch !-------------------------whit id "
        # letter += str(rows[idx][0])
        # letter += "--------------------------\n"
        # print(letter)

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
            cur.execute("update persons set first_name = ?, last_name = ?, country = ?,  profession = ? where id = ?",
                (list_temp[1],list_temp[2],list_temp[3],list_temp[4],list_temp[0]))
            
            #  (rows[idx][0], rows[idx][1],rows[idx][2],rows[idx][3],rows[idx][4]))
        #con.commit()
    finally:
        lock.release()
#-------------------------Get the Excel--------------------------------
file = open("workload100.csv")
csvreader = csv.reader(file)
header = next(csvreader)
rows = []
for row in csvreader:
    rows.append(row)
file.close()

print("################################################## SQLite Benchmark ######################################################\n")

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
avg_time = 0

total_num_read = 0
total_num_delete = 0
total_num_update = 0
total_num_insert = 0
#------------------Main loop for batchs---------------------------------
while num_tasks > 0:
    start = time. time()
    msg = "\n#################################### Batch number: "
    msg += str(count)
    msg +=" ####################################"
    print(msg)

    num_read = 0
    num_delete = 0
    num_update = 0
    num_insert = 0

    threads = []

    if num_tasks <= MAX_THREAD:
        num_threads = num_tasks
    else:
        num_threads = MAX_THREAD


    for d in range(num_threads):
        if rows[d+prev_threads][6] == 'read':
            num_read += 1
        elif rows[d+prev_threads][6] == 'delete':
            num_delete += 1
        elif rows[d+prev_threads][6] == 'update':
            num_update += 1
        elif rows[d+prev_threads][6] == 'insert':
            num_insert += 1

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

    end = time. time()
    avg_time += end - start

    msg_p = "\nNo. Operations, "
    msg_p += "insert,  update,  read,  delete,  time \n     "
    msg_p += str(num_threads)
    msg_p += ",       "
    msg_p += str(round((float(num_insert)/float(num_threads))*100,2))
    msg_p += "%,   "
    msg_p += str(round((float(num_update)/float(num_threads))*100,2))
    msg_p += "%,  "
    msg_p += str(round((float(num_read)/float(num_threads))*100,2))
    msg_p += "%,  "
    msg_p += str(round((float(num_delete)/float(num_threads))*100,2))
    msg_p += "%, "
    msg_p += str(round((end - start),2))
    msg_p += " seconds"

    print(msg_p)
    #------Uptdate the actual number of tasks ramaining to do for the next batch----------
    total_num_read += num_read
    total_num_delete += num_delete
    total_num_update += num_update
    total_num_insert += num_insert
    num_tasks = num_tasks - num_threads
    prev_threads += num_threads
    count += 1


# Insert a row of data
#cur.execute("INSERT INTO persons VALUES ('1','BUY','RHAT','israel','35.14')")

# Save (commit) the changes
print("\n**********************************************Result Benchmark***********************************")
msg_t = "\nTotal no. Operations: "
msg_t += str(len(rows))
msg_t += "\nGlobal avg of insert: "
msg_t += str(round((float(total_num_insert/float(len(rows))))*100,2))
msg_t += "%,    "
msg_t += "\nGlobal avg of update: "
msg_t += str(round((float(total_num_update/float(len(rows))))*100,2))
msg_t += "%,    "
msg_t += "\nGlobal avg of read  : "
msg_t += str(round((float(total_num_read/float(len(rows))))*100,2))
msg_t += "%,    "
msg_t += "\nGlobal avg of delete: "
msg_t += str(round((float(total_num_delete/float(len(rows))))*100,2))
msg_t += "%,    "
msg_t += "\nGlobal avg of time: "
msg_t += str(round(float(avg_time)/float(count),2))
msg_t +=" seconds"
print(msg_t)

con.commit()

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
con.close()