AUTHORS
========
Nalin Gupta - 2014065
Sahar Siddiqui - 2014091

FILES
======
1. params.py - This file contains all the global variables governing the building of the indexes, disk blocks and synthetic table. These values need to be set before creating anything else. 

2. setup.py - This file needs to be run first. This used the system specification put in params.py and builds the database. It then stores the database in disk blocks. Once the database is stored in disk bocks it indexes the data using three schemes - ROW_ID indexing, bitmap indexing and bitslice indexing. 

3. main.py - This file is used to evaluate the three indexing schemes, it defines 6 different values of records to be retrieved and sees how manu disk blocks have been accessed in each indexing scheme. 

USAGE
======

1. Set all the variables in params.py (By default the values written in the assignment have been set)
2. Run setup.py
		$python setup.py
	- This code will take about an hour to create the database and then index it for 20,00,000 values. 
	- This will create a file called file_index.txt which will contain the file_name of the next file (disk block) to be used.
	- It will also create 3 pickled files for the secondary index of row_id, bitmap and bit-slice. 
3. Run main.py
		$python main.py
	- This code will take about 1.5 - 2 hrs to run for 20,00,000 values. 

REQUIREMENTS
=============
1. math
2. string
3. pickle 
4. random
5. __future__.division