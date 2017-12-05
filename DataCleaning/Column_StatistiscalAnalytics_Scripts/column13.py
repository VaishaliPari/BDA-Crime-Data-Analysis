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

#Function to check validity of NYC barough names x[13]
def is_valid_borough_name(x):
    if x is "" or x is " ":
        return "NULL"
    else:
        borough_names_list=["MANHATTAN",'BRONX',"BROOKLYN","QUEENS","STATEN ISLAND"]
        if x not in borough_names_list:
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
	col=data.map(lambda x : (x[13],is_valid_borough_name(x[13])));
	#count "Valid" "Invalid" or "NULL"
	count=col.map(lambda x: (x[1],1)).reduceByKey(lambda x,y:x+y).collect();
	cnt=sc.parallelize(count);
	#output the specific column and the counts
	col.saveAsTextFile('NYPD_data_column13.out');
	cnt.saveAsTextFile('NYPD_count_column13.out');

