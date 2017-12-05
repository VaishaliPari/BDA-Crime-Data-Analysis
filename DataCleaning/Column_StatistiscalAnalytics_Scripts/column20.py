from __future__ import print_function
from pyspark import SparkContext
from csv import reader

import sys

import re
import datetime

#Function to check y coordinate for x[20]
#Y-coordinate (North-South) NYC
# minimum: 117500; maximum: 275000
def to_check_y_coord(x):
	try:
		x=int(x);
		if x== "" or x== " "or x=="\t":
			return "NULL"
		elif((117500<x) & (x<275000)):
			return "VALID"
		else:
			return "INVALID"
	except ValueError:
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
	col=data.map(lambda x : (x[20],to_check_y_coord(x[20])));
	#count "Valid" "Invalid" or "NULL"
	count=col.map(lambda x: (x[1],1)).reduceByKey(lambda x,y:x+y).collect();
	cnt=sc.parallelize(count);
	#output the specific column and the counts
	col.saveAsTextFile('NYPD_data_column20.out');
	cnt.saveAsTextFile('NYPD_count_column20.out');
