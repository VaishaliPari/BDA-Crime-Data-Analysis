from __future__ import print_function
from pyspark import SparkContext
from csv import reader

import sys

import re
import datetime

class Point:
    def __init__(self,x,y):
        self.x = x
        self.y = y

#Function to check validity of key code and PD_CD x[6] and x[8]
def is_valid_key_code(x):
	if x is "" or x is " ":
		return "NULL"
    #check using regular expression
	elif re.match('[0-9][0-9][0-9]$',x):
		return "VALID"
	else :
		return "INVALID"

if __name__ == "__main__":
	sc=SparkContext();
	data = sc.textFile(sys.argv[1], 1);
	data = data.mapPartitions(lambda x : reader(x));
	#extract header
	header = data.first();
	#removing the header
	data = data.filter(lambda x : x != header);
	#extract the required column
	col=data.map(lambda x : (x[8],is_valid_key_code(x[8])));
	#count "Valid" "Invalid" or "NULL"
	count=col.map(lambda x: (x[1],1)).reduceByKey(lambda x,y:x+y).collect();
	cnt=sc.parallelize(count);
	#output the specific column and the counts
	col.saveAsTextFile('NYPD_data_column8.out');
	cnt.saveAsTextFile('NYPD_count_column8.out');

