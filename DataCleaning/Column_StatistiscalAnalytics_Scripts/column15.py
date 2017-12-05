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

#Function to check validity of locations_list of occurence x[15]
def is_valid_loc_of_occurance(x):
    if x is "" or x is " ":
        return "NULL"
    else:
        locations_list=['FRONT OF','OPPOSITE OF','REAR OF','INSIDE','OUTSIDE']
        if x not in locations_list:
            return "INVALID"
        else :
            return "VALID"

if __name__ == "__main__":
	sc=SparkContext();
	data = sc.textFile(sys.argv[1], 1);
	data = data.mapPartitions(lambda x : reader(x));
	#extract header
	header = data.first();
	#removing the header
	data = data.filter(lambda x : x != header);
	#extract the required column
	col=data.map(lambda x : (x[15],is_valid_loc_of_occurance(x[15])));
	#count "Valid" "Invalid" or "NULL"
	count=col.map(lambda x: (x[1],1)).reduceByKey(lambda x,y:x+y).collect();
	cnt=sc.parallelize(count);
	#output the specific column and the counts
	col.saveAsTextFile('NYPD_data_column15.out');
	cnt.saveAsTextFile('NYPD_count_column15.out');

