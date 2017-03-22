###########################################################
#	Authors 	Nalin Gupta		2014065
#				Sahar Siddiqui	2014091
#	
#	Setting up the Database System by creating 
#	a synthetic table and storing that in Disk blocks.
#	Once the disk blocks are created, the data is stored
#	using ROW_ID index, bitmap index and bit-slice index.
#
#	Usage:
#			python setup.py
#
#
############################################################

import string
import random
import params
import pickle


def create_dataset():
	print ("#Creating synthetic database")
	chars = string.letters
	pwdSize = params.CUSTOMER_NAME_LENGTH

	customer_name = []
	sales_amount = []

	#Generating random values for the synthetic table
	for i in range(0,params.TABLE_SIZE):
		word = ''.join((random.choice(chars)) for x in range(pwdSize))
		customer_name.append(word.lower())

		sales_amount.append(random.randint(1, params.MAX_SALES_AMOUNT))

	#Populating the synethetic table
	file = open('dataset.txt','w+')
	for i,s,c in zip(range(1,params.TABLE_SIZE + 1),sales_amount,customer_name):
		file.write(str(i) + "\t" + str(s) + "\t" + c + "\n")

	print("#Database created:")
	print("Database size: " + str(params.TABLE_SIZE) + "\n")

	#Create file_index.txt to store next file index
	file = open(params.FILE_INDEX,"w+")
	file.write("1")
	file.close()

def create_disk_blocks():
	print("#Writing synthetic database to disk blocks")
	with open('dataset.txt','r') as f:
		data = f.readlines()

	#Get index of file which is to be populated
	fi_file = open(params.FILE_INDEX,"r")
	file_index = int(fi_file.readlines()[0])
	fi_file.close()

	#Create disk blocks and write data to it
	for i in range(0,len(data),params.TABLE_RECORD_BLOCKING_FACTOR):
		f = open(params.DATA_BASE_DIR + str(file_index)+".txt","w+")
		file_index+=1
		#Case where number of remaining records is greater than blocking factor
		if i+params.TABLE_RECORD_BLOCKING_FACTOR<len(data):
			for j in range(i,i+params.TABLE_RECORD_BLOCKING_FACTOR):
				f.write(data[j])
			f.write(params.POINTER + str(file_index))
		#Case where records can fit into the disk block without exceeding blocking factor
		else:
			for j in range(i,len(data)):
				f.write(data[j])
			f.write(params.POINTER)

	print("#Disk blocks created")
	print("Disk blocks used: " + str(file_index-1) + "\n")

	#Update next file index in file_index.txt
	fi_file = open(params.FILE_INDEX,"w")
	fi_file.write(str(file_index))
	fi_file.close()

def create_bitmap():
	print("#Creating row_id index")

	#Initializing hahsmap for ROW_ID indexing
	row_id_dict= dict()
	row_hashmap = dict()

	with open('dataset.txt','r') as f:
		data = f.readlines()

	#Populating dictionary storing amount and corresponding ROW_IDS
	for i in range(0,len(data)):
		data_parts = data[i].split("\t")
		amount = int(data_parts[1])
		if amount in row_id_dict.keys():
			row_id_dict[amount].append(i+1)
		else:
			row_id_dict[amount] = []
			row_id_dict[amount].append(i+1)

	#Get next file index
	fi_file = open(params.FILE_INDEX,"r")
	file_index = int(fi_file.readlines()[0])

	#Create disk blocks and write ROW_IDs to it
	for amount in sorted(row_id_dict.iterkeys()):
		# print str(file_index) + " " + str(amount)
		row_hashmap[amount] = file_index
		for i in range(0,len(row_id_dict[amount]),params.ROW_ID_BLOCKING_FACTOR):
			f = open(params.DATA_BASE_DIR + str(file_index)+".txt","w+")
			file_index+=1
			#Case where number of remaining records is greater than blocking factor
			if i+params.ROW_ID_BLOCKING_FACTOR<len(row_id_dict[amount]):
				for j in range(i,i+params.ROW_ID_BLOCKING_FACTOR):
					f.write(str(row_id_dict[amount][j]) + "\n")
				f.write(params.POINTER + str(file_index))
			#Case where records can fit into the disk block without exceeding blocking factor
			else:
				for j in range(i,len(row_id_dict[amount])):
					f.write(str(row_id_dict[amount][j]) + "\n")
				f.write(params.POINTER)

	print("#Row_id index created with secondary index\n")
	# print row_hashmap

	#Storing the secondary index of ROW_ID indexing as a pickled file
	with open('row_hashmap.pickle', 'wb') as handle:
		pickle.dump(row_hashmap, handle, protocol=pickle.HIGHEST_PROTOCOL)


	print("#Creating bitmap index")
	#Initializing hahsmap for bitmap indexing
	bit_array_dict= dict()
	bit_hashmap = dict()

	#Populating dictionary storing amount and corresponding bitmap
	for i in range(0,len(data)):
		data_parts = data[i].split("\t")
		amount = int(data_parts[1])
		data_id = int(data_parts[0])
		if amount in bit_array_dict.keys():
			bit_array_dict[amount][data_id-1] = 1
		else:
			bit_array_dict[amount] = [0] * params.TABLE_SIZE
			bit_array_dict[amount][data_id-1] = 1

	#Create disk blocks and write bitmaps to it
	for amount in sorted(bit_array_dict.iterkeys()):
		# print str(file_index) + " " + str(amount)
		bit_hashmap[amount] = file_index
		for i in range(0,len(bit_array_dict[amount]),params.BIT_ARRAY_BLOCKING_FACTOR):
			f = open(params.DATA_BASE_DIR + str(file_index)+".txt","w+")
			file_index+=1
			#Case where number of remaining records is greater than blocking factor
			if i+params.BIT_ARRAY_BLOCKING_FACTOR<len(bit_array_dict[amount]):
				for j in range(i,i+params.BIT_ARRAY_BLOCKING_FACTOR):
					f.write(str(bit_array_dict[amount][j]) + "\n")
				f.write(params.POINTER + str(file_index))
			#Case where records can fit into the disk block without exceeding blocking factor
			else:
				for j in range(i,len(bit_array_dict[amount])):
					f.write(str(bit_array_dict[amount][j]) + "\n")
				f.write(params.POINTER)

	print("#Created bitmap index with secondary index\n")
	# print bit_hashmap

	#Storing the secondary index of bitmap indexing as a pickled file
	with open('bit_hashmap.pickle', 'wb') as handle:
		pickle.dump(bit_hashmap, handle, protocol=pickle.HIGHEST_PROTOCOL)

	#Update next file index in file_index.txt
	fi_file = open(params.FILE_INDEX,"w")
	fi_file.write(str(file_index))

def create_bitslice():
	print("#Creating bitslice index")
	bitslice_index = []

	#Initializing hahsmap for bit-slice indexing
	bitslice_hashmap = dict()

	#Generating empty slices 
	for i in range(0,params.NO_OF_BITS):
		temp_slice = [0] * params.TABLE_SIZE
		bitslice_index.append(temp_slice)

	with open('dataset.txt','r') as f:
		data = f.readlines()

	#Creating slices based on the binary representation of sales
	for i in range(0,len(data)):
		data_parts = data[i].split("\t")
		amount = int(data_parts[1])
		data_id = int(data_parts[0])
		bits_rep = bin(amount)[2:].zfill(params.NO_OF_BITS)
		for j in range(0,params.NO_OF_BITS):
			if bits_rep[j] != '0':
				bitslice_index[params.NO_OF_BITS - j - 1][data_id-1] = 1

	#Retreiving next file index
	fi_file = open(params.FILE_INDEX,"r")
	file_index = int(fi_file.readlines()[0])

	#Create disk blocks and write bitslices to it
	for i in range(0,len(bitslice_index)):
		# print str(file_index) + " " + str(i)
		bitslice_hashmap[i] = file_index
		for j in range(0,len(bitslice_index[i]),params.BIT_SLICE_BLOCKING_FACTOR):
			f = open(params.DATA_BASE_DIR + str(file_index)+".txt","w+")
			file_index+=1
			if j+params.BIT_SLICE_BLOCKING_FACTOR<len(bitslice_index[i]):
				for k in range(j,j+params.BIT_SLICE_BLOCKING_FACTOR):
					f.write(str(bitslice_index[i][k]) + "\n")
				f.write(params.POINTER + str(file_index))
			else:
				for k in range(j,len(bitslice_index[i])):
					f.write(str(bitslice_index[i][k]) + "\n")
				f.write(params.POINTER)

	print("#Created bitslice index with secondary index\n")
	# print bitslice_hashmap

	#Storing the secondary index of bitslice indexing as a pickled file
	with open('bitslice_hashmap.pickle', 'wb') as handle:
		pickle.dump(bitslice_hashmap, handle, protocol=pickle.HIGHEST_PROTOCOL)

	#Update next file index in file_index.txt
	fi_file = open(params.FILE_INDEX,"w")
	fi_file.write(str(file_index))

#Creates initial values for synthetic database and stores it in dataset.txt
create_dataset()

#Splits the dataset in blocks defined by the blocking factor. 
create_disk_blocks()

#Creates the ROW_ID index and bitmap index with secondary indexing on both. 
create_bitmap()

#Creates bitslice index with secondary indexing.
create_bitslice()

