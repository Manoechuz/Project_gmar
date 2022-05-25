import pandas as pd
import numpy as np
import csv
import random
import time
import datetime


num_row = int(input("Enter number of rows you want: "))

with open('workload.csv', 'w',newline="") as f:
	writer = csv.writer(f)
	writer.writerow(["id","first_name","last_name","country","professor","last_modif","action"])
	list_action = ["insert","delete","uptdate"]
	list_consone = ["q","w","r","t","p","s","d","f","g","h","j","k","l","m","n","b","v","c","x","z"]
	list_voyel = ["a","e","y","u","i","o"]
	complete_list = []
	for row in range(num_row):
		temp_list = []
		check = len(complete_list)
		# num of col there is
		for co in range(7):

			if co == 5:# put the time stamp
				temp_list.append(datetime.datetime.now())
				continue
			elif co == 6:
				u = random.randint(0,2)

				#if we have an delete action
				if u > 0 and check > 0:
						k = random.randint(0,check-1)
						temp_list = [] # add an existing row
						for i in range(len(complete_list[k])-1):
							temp_list.append(complete_list[k][i])
						temp_list.append(list_action[u])
				else:
					temp_list.append(list_action[0])
				continue
			#define a random size for the word include the last num in random
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
		writer.writerows(complete_list)
	for i in complete_list:
		print(i)

