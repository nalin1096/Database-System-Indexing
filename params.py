#################################################
#	Authors 	Nalin Gupta		2014065
#				Sahar Siddiqui	2014091
#	
#	Storing global paramters for the system
#
#	Usage:
#			import params
#			params.{variable_name}
#
#
##################################################

import math

#variables defining the properties of the synthetic table
TABLE_SIZE = 2000000
CUSTOMER_NAME_LENGTH = 3
MAX_SALES_AMOUNT = 2500

#Defines how the next disk block will be defined at the end of a previous disk block
POINTER = "#"

#Path to the file where the index of the current file to be populated will be stored
FILE_INDEX = './file_index.txt'

#Base directory where the disk blocks will be stored
DATA_BASE_DIR = './'

#Blocking factors for each index/record
TABLE_RECORD_BLOCKING_FACTOR = 300
ROW_ID_BLOCKING_FACTOR = 1000
BIT_ARRAY_BLOCKING_FACTOR = 32000
BIT_SLICE_BLOCKING_FACTOR = 32000

#Calculating no of bits required to represent the sales amount
NO_OF_BITS = int(math.ceil(math.log(MAX_SALES_AMOUNT,2)))
