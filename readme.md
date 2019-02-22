# Requirements to run code
 - Minimum jdk 1.7
 - One ide like eclipse or IntellIj
 - One csv file in directory "csvFiles" with name "Interview-task-data-osh.csv"

# How to run code
 - Import project as "Project from folder" to an ide like Eclipse or IntellIj and run main class
 - If is necessary add library from library folder to this project
 
 # Result
 - In csvFile folder after execution you will be able to see log file and bad data file
 
 # Solution to solve task
 	To read file i use opencsv library that give me possibility to read file without saving content in memory,
	but save tonly row that is readed at moment, when go to next record, previous record is removed from memory.
	I chose this functionality because file can be very large.After reading record i check if contains "null"
	or empty elements and depending of this condition i insert record in file with bad records or in DB.
	For rectords inserted in DB i also check if one of column contains comma and add double quotes on start and
	end of this column content. Also every readed record and inserted in file or DB is counted by three counters
	that just add one value to previous counter when is one of action mentioned above. 