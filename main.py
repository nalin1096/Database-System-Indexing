###########################################################
#	Authors 	Nalin Gupta		2014065
#				Sahar Siddiqui	2014091
#	
#	Testing the different indexing methods (no index, bitmap,
#	bitslice and ROW_ID) on the basis of number of disk
#	blocks retreieved. 
#
#	Usage:
#			python main.py
#
#
############################################################

from __future__ import division
import random
import params
import math
import pickle

#Function which searched for a record in a disk block
def search_row_id(row_num,data):
	if (str(row_num) in data):
		return True
	return False

#function which converts row_id to corresponding file index and entry within the file 
def rowid_file(idx):
	file = math.floor(idx/params.TABLE_RECORD_BLOCKING_FACTOR)
	entry = idx%params.TABLE_RECORD_BLOCKING_FACTOR
	return int(file+1),int(entry)

def no_index(bf):
	#intialziing blocks read and total amount
	amount_sum = 0
	blocks_read = 0

	#Storing row_ids of all data points to be retrieved from bf
	bf_arr = []
	for idx,b in enumerate(bf):
		if b != 0:
			bf_arr.append(idx)

	"""Generating a dictionary which maps a file to the entries from that file 
	which need to retrieved. This is done by converting each row_id to be retrieved 
	to its corresponding file and entry in file"""
	file_mappings = dict()
	for b in bf_arr:
		file, entry = rowid_file(b)
		if file in file_mappings:
			file_mappings[file].append(entry)
		else:
			file_mappings[file] = []
			file_mappings[file].append(entry)

	#Iterate through all the the files where some record which needs to retrieved exists
	for file in file_mappings.keys():
		f = open(params.DATA_BASE_DIR + str(file) + ".txt","r")
		blocks_read +=1
		data = f.readlines()
		f.close()
		#Iterate through all the entries in the file which needs to be retrieved
		for entry in file_mappings[file]:
			datapoint = data[entry]
			data_split = datapoint.split("\t")
			amount = data_split[1]
			amount_sum += int(amount)
	return blocks_read, amount_sum


def row_id(bf):
	#intialziing blocks read and total amount
	amount_sum = 0
	blocks_read = 0

	#Reading the secondary index of row_id indexing into main memory
	with open('row_hashmap.pickle', 'r') as handle:
		row_hashmap = pickle.load(handle)
	
	#Storing row_ids of all data points to be retrieved from bf
	bf_arr = []
	for idx,b in enumerate(bf):
		if b != 0:
			bf_arr.append(idx+1)
	
	
	"""We iterate through all the keys in the secondary index of the row_id indexing (sales amounts)
	until we have retrieved all the records in bf. Some heuristics which are in place are:
	- If the next record to be retrieved is greater than the last record of a disk block, we skip the 
	disk block
	- We dont retrieve the same disk block twice, as we check for all remaining records to be retrieved in each 
	disk block. 
	- global_ctr keeps track of how many records have been retrieved, once this number is equal to the number of records
	to be retrieved, we stop the search"""
	global_ctr = 0
	for sales in row_hashmap.keys():
		ctr = 0
		file = row_hashmap[sales]
		f = open(params.DATA_BASE_DIR + str(file) + ".txt","r")
		blocks_read +=1
		data = f.readlines()
		data = [x.strip() for x in data] 
		
		"""The loop runs till the last record hasnt been processed or we have reached the end of the disk blocks
		for a particular sales amount""" 
		while ctr<len(bf_arr):
			last_char = data[len(data)-1]
			#Condition where the disk block is the last disk block for a particular sales amount
			if last_char == params.POINTER:
				for i in range(ctr,len(bf_arr)):
					flag = search_row_id(bf_arr[i],data)
					if (flag):
						global_ctr +=1
						ctr = i+1
						amount_sum += sales
				break
			#Case where the last record of the disk block is less than the next record to be searched 
			if data[len(data)-2] < bf_arr[ctr]:
				next_page = last_char[1:]
				f.close()
				f = open(params.DATA_BASE_DIR + str(next_page) + ".txt","r")
				blocks_read +=1
				data = f.readlines()
				data = [x.strip() for x in data] 
			"""Case where the last record of the disk block is gretaer than the next record to be searched and hence record
			may exist in the disk block"""
			else:
				for i in range(ctr,len(bf_arr)):
					flag = search_row_id(bf_arr[i],data)
					if (flag==1):
						global_ctr +=1
						ctr = i+1
						amount_sum += sales
		if (global_ctr==len(bf_arr)):
			break
	return blocks_read,amount_sum

def bitslice(bf):
	#intialziing blocks read and total amount
	amount_sum = 0
	blocks_read = 0
	
	#Generating a bit string for bf
	bf_str = ""
	for i in bf:
		bf_str += str(i)

	#Reading the secondary index of bitslice indexing into main memory
	with open('bitslice_hashmap.pickle', 'r') as handle:
		bitslice_hashmap = pickle.load(handle)	

	"""We iterate over the keys of the secondary index of bitslice indexing (significant bits) and perform 
	a bitwise AND with the bf bits vector. We then multiply the sum of the 1's in the resulting vector with 
	2^(significant_bit_index)"""
	for sig_bit in bitslice_hashmap.keys():
		file = bitslice_hashmap[sig_bit]
		f = open(params.DATA_BASE_DIR + str(file) + ".txt","r")
		blocks_read +=1
		bit_array = ""

		last_char = ""
		while last_char!=params.POINTER:
			data = f.readlines()
			data = [x.strip() for x in data] 
			last_char = data[len(data)-1]
			data = data[:len(data)-1]

			for bit in data:
				bit_array += bit

			if last_char !=params.POINTER:
				f.close()
				next_file = last_char[1:]
				f = open(params.DATA_BASE_DIR + str(next_file) + ".txt","r")
				blocks_read +=1

		bitwise_and = bin(int(bf_str,2) & int(bit_array,2))[2:].zfill(params.TABLE_SIZE)
		bitwise_list = [int(i) for i in bitwise_and]
		bitwise_sum = sum(bitwise_list)
		amount_sum += (math.pow(2,sig_bit) * bitwise_sum)
	return blocks_read, int(amount_sum)

def bitmap(bf):
	#intialziing blocks read and total amount
	amount_sum = 0
	blocks_read = 0
	
	#Generating a bit string for bf
	bf_str = ""
	for i in bf:
		bf_str += str(i)

	#Reading the secondary index of bitmap indexing into main memory
	with open('bit_hashmap.pickle', 'r') as handle:
		bit_hashmap = pickle.load(handle)	

	"""We iterate over the keys of the secondary index of bitmap indexing (sales amount) and perform 
	a bitwise AND with the bf bits vector. We then multiply the sum of the 1's in the resulting vector with 
	corresponding sales amount"""
	for sales in bit_hashmap.keys():
		print sales
		file = bit_hashmap[sales]
		f = open(params.DATA_BASE_DIR + str(file) + ".txt","r")
		blocks_read +=1
		bit_array = ""

		last_char = ""
		while last_char!=params.POINTER:
			data = f.readlines()
			data = [x.strip() for x in data] 
			last_char = data[len(data)-1]
			data = data[:len(data)-1]

			for bit in data:
				bit_array += bit

			if last_char !=params.POINTER:
				f.close()
				next_file = last_char[1:]
				f = open(params.DATA_BASE_DIR + str(next_file) + ".txt","r")
				blocks_read +=1

		bitwise_and = bin(int(bf_str,2) & int(bit_array,2))[2:].zfill(params.TABLE_SIZE)
		bitwise_list = [int(i) for i in bitwise_and]
		bitwise_sum = sum(bitwise_list)
		amount_sum += (sales * bitwise_sum)
	return blocks_read, int(amount_sum)

def evaluation (dist):
	#Initializing the bf array
	bf = [0] * params.TABLE_SIZE
	arr = []

	#Generating random indexes to set to 1
	ctr = 0
	while ctr<dist:
		rn = random.randint(0, params.TABLE_SIZE - 1)
		if rn not in arr:
			arr.append(rn)
			ctr+=1
	for ar in arr:
		bf[ar] = 1

	print "Evaluation - Retreiving " + str(dist) + " records: "
	
	#Running the evaluation for no index scheme 
	ni_br, ni_ams = no_index(bf)
	print "Amount: " + str(ni_ams)
	print "No-Index: " + str(ni_br)
	
	#Running the evaluation for ROW_ID indexing 
	ri_br, ri_ams = row_id(bf)
	print "Row_ID indexing: " + str(ri_br)

	#Running the evaluation for bitmap scheme 
	bm_br, bm_ams = bitmap(bf)
	print "Bitmap indexing: " + str(bm_br)
	
	#Running the evaluation for bit-slice scheme 
	bs_br, bs_ams = bitslice(bf)
	print "Bit-Slice indexing: " + str(bs_br)
	print "\n"

#Evaluating the different indexing schemes for different number records to retrieve
records_to_retrieve = [100000,10000,2000,500,100,10]
for val in records_to_retrieve:
	evaluation(val)