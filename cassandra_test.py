import csv
from cassandra.cluster import Cluster
from decimal import Decimal
import numpy

file = open("po_table.csv")
csvreader = csv.reader(file)
header = next(csvreader)
#print(header)
rows = []
for row in csvreader:
    rows.append(row)
#list_supp = rows.copy()
#for li in list_supp:
#    list_supp.remove(li)
#    if li in list_supp:
#        print('cafoule')
     
#print(rows)
file.close()
#print(rows[0][21])
cluster = Cluster()

session = cluster.connect('test_keyspace')


#session.execute("CREATE TABLE python_test(id int PRIMARY KEY, name text, position text)")
#session.execute("CREATE TABLE po_table1(Orders text, Line int,Site text,Supplier text, Name text,Item_Number int PRIMARY KEY, Quantity_Ordered int, Quantity_Open int,Quantity_Received int, UM text, Due_Date text,Sales_Job text, Producti text, St text, ERS_PL_Opt int, ERS_Opt int, Consignment text, Type text, Operation int,Ship_To text,Performance_Date text, Nee_Date text,Order_Revision int, Item_Description text)")
for i in range(1,len(rows)):
	for j in range(len(rows[i])):
		if rows[i][j] == '':
			rows[i][j] = '0'
		if j == 1 or j == 5 or j == 6 or j == 7 or j == 8 or j == 14 or j == 15 or j == 18 or j == 22:
			numeric_filter = filter(str.isdigit, rows[i][j])
			rows[i][j] = "".join(numeric_filter)
	test = int(float(rows[i][1]))
	test0 = int(float(rows[i][5]))
	test1 = int(float(rows[i][6]))
	test2 = int(float(rows[i][7]))
	test3 = int(float(rows[i][8]))
	test4 = int(float(rows[i][14]))
	test5 = int(float(rows[i][15]))
	test6 = int(float(rows[i][18]))
	test7 = int(float(rows[i][22]))
	session.execute(
		"""INSERT INTO po_table1(Orders,Line,Site,Supplier, Name,Item_Number, Quantity_Ordered, Quantity_Open,Quantity_Received, UM, Due_Date,Sales_Job, Producti, St, ERS_PL_Opt, ERS_Opt , Consignment, Type, Operation,Ship_To,Performance_Date, Nee_Date ,Order_Revision, Item_Description)
		VALUES (%(Orders)s, %(Line)s, %(Site)s, %(Supplier)s, %(Name)s, %(Item_Number)s, %(Quantity_Ordered)s, %(Quantity_Open)s, %(Quantity_Received)s, %(UM)s, %(Due_Date)s, %(Sales_Job)s, %(Producti)s, %(St)s, %(ERS_PL_Opt)s, %(ERS_Opt)s, %(Consignment)s, %(Type)s, %(Operation)s,%(Ship_To)s, %(Performance_Date)s, %(Nee_Date)s, %(Order_Revision)s, %(Item_Description)s)""",
		{'Orders': rows[i][0], 'Line': test, 'Site': rows[i][2], 'Supplier' : rows[i][3], 'Name' : rows[i][4], 'Item_Number' : test0, 'Quantity_Ordered' : test1, 'Quantity_Open' : test2,'Quantity_Received' : test3, 'UM' : rows[i][9], 'Due_Date' : rows[i][10],'Sales_Job' : rows[i][11], 'Producti': rows[i][12], 'St' : rows[i][13], 'ERS_PL_Opt' : test4, 'ERS_Opt' : test5 , 'Consignment' : rows[i][16], 'Type' : rows[i][17], 'Operation' : test6,'Ship_To': rows[i][19],'Performance_Date' : rows[i][20], 'Nee_Date' : rows[i][21] ,'Order_Revision' : test7, 'Item_Description' : rows[i][23]}
		)
print("finished")