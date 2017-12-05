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

#Function to check validity of Time x[2] and x[4]
def is_valid_time(x):
    if x is "" or x is " ":
        return "NULL"
    else :
            y=x
            x=x.split(":")
            try:
                hour=int(x[0])
                mins=int(x[1])
                secs= int(x[2])
                # if hours is 24 then change it to 0 hours
                #if hour == 24 and mins== 0 and secs == 0:
                 #   hour=0
                try:
                        newTime= datetime.time(hour,mins,secs)
                        return "VALID"
                except :
                        return "INVALID"
            except:
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
	col=data.map(lambda x : (x[2],is_valid_time(x[2])));
	#count "Valid" "Invalid" or "NULL"
	count=col.map(lambda x: (x[1],1)).reduceByKey(lambda x,y:x+y).collect();
	cnt=sc.parallelize(count);
	#output the specific column and the counts
	col.saveAsTextFile('NYPD_data_column2.out');
	cnt.saveAsTextFile('NYPD_count_column2.out');

