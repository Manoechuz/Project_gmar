import pandas as pd
import numpy as np
import csv
import random
import time
import datetime
from random import shuffle

num_row = int(input("Enter number of rows you want: "))

with open('workload.csv', 'w', newline="") as f:
	writer = csv.writer(f)
	writer.writerow(["id","first_name","last_name","country","profession","last_modif","action"])
	list_action = ["insert","delete","update","read"]
	list_consone = ["q","w","r","t","p","s","d","f","g","h","j","k","l","m","n","b","v","c","x","z"]
	list_voyel = ["a","e","y","u","i","o","-"]
	complete_list = []

	for row in range(num_row):
		temp_list = []
		check = len(complete_list)
		# num of col there is
        
		for co in range(7):
			if co == 5:# put the time stamp
				time.sleep(0.001)               
				temp_list.append(int(time.time() * 1e6))
				continue
			elif co == 6:
                #Chosing an action
				u = random.randint(0,3)
				#if we have a delete or update action
				if u > 0 and check > 0:
					k = random.randint(0,check-1)
					if u != 2: #if its a delete action or read
						temp_list = [] # add an existing row
						for i in range(len(complete_list[k])-2):
							temp_list.append(complete_list[k][i])
						time.sleep(0.001)                            
						temp_list.append(int(time.time() * 1e6))#add current timestamp
						temp_list.append(list_action[u])
					else:#if its an uptdate action
						temp_list[0] = complete_list[k][0]#add current id
						ct = 0
						for rl in complete_list:# check if the last action of the current id was delete
							if rl[0] == complete_list[k][0] and rl[6] != 'read':
								ct += 1
						for rl1 in complete_list:
							if rl1[0] == complete_list[k][0] and rl1[6] != 'read':
								ct -= 1
							if ct == 0:
								if rl1[6] == 'delete':
									temp_list.append('insert')
								else:
									temp_list.append(list_action[u])# add current action
								break
				else:
					temp_list.append(list_action[0])
				continue
			#define a random size for the word include the last num in random
			elif co == 0:
				temp_list.append(row)
				continue
			n = random.randint(4,22)

			#the random word for the current column
			temp = ""

			#define if need a consone or voyel
			flag = True

			for i in range(2,n): # minimum size of the word is length of 2

				if flag:
					temp += random.choice(list_consone)
					flag = False
				else:

					temp += random.choice(list_voyel)
					flag = True
			temp_list.append(temp)
		complete_list.append(temp_list)
	#shuffle(complete_list) # mix out the final list of the workload
	writer.writerows(complete_list)
	print("\nYOUR WORKLOAD IS READY :-)")
    
	check = True
	continue_output = str(input("Would you like to see him ? (y/n) "))
	while check:
		if continue_output == 'y' or continue_output == 'n':
			check = False
			continue
		continue_output = str(input("Please enter only 'y' or 'n' caracters !\n"))
	if continue_output == 'y':
		for i in complete_list:
			print(i)

