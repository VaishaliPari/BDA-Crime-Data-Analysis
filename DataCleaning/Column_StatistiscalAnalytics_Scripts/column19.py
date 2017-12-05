from __future__ import print_function
from pyspark import SparkContext
from csv import reader

import sys

import re
import datetime

#Function to check x coordinate for x[19]
#X-coordinate (East-West) NYC
#minimum: 909900; maximum: 1067600
def to_check_x_coord(x):
	try:
		x=int(x);
		if x== "" or x== " "or x=="\t":
			return "NULL"
		elif((909900<x) & (x<1067600)):
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
	col=data.map(lambda x : (x[19],to_check_x_coord(x[19])));
	#count "Valid" "Invalid" or "NULL"
	count=col.map(lambda x: (x[1],1)).reduceByKey(lambda x,y:x+y).collect();
	cnt=sc.parallelize(count);
	#output the specific column and the counts
	col.saveAsTextFile('NYPD_data_column19.out');
	cnt.saveAsTextFile('NYPD_count_column19.out');
