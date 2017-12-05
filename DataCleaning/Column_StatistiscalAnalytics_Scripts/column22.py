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

#Bounding Box Long details for NYC
long_max=-73.700009
long_min=-74.25909

def to_check_long(x):
	try:
		x=float(x);
		if x== "" or x== " "or x=="\t":
			return "NULL"
	        elif((long_min<x) & (x<long_max)):
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
	col=data.map(lambda x : (x[22],to_check_long(x[22])));
	#count "Valid" "Invalid" or "NULL"
	count=col.map(lambda x: (x[1],1)).reduceByKey(lambda x,y:x+y).collect();
	cnt=sc.parallelize(count);
	#output the specific column and the counts
	col.saveAsTextFile('NYPD_data_column22.out');
	cnt.saveAsTextFile('NYPD_count_column22.out');
