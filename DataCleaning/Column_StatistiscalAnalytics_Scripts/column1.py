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

#Function to check validity of date for x[1], x[3] and x[5]
def is_valid_date(x):
	if x is "" or x is " ":
		return "NULL"
	else :
            y=x
            x=x.split("/")
            try:
                year=int(x[2])
                month=int(x[0])
                day= int(x[1])
                #year specifically between 2006 and 2016, others are invalid
                if year >=2006 and year <=2016 :
                    try:
                        newDate= datetime.datetime(year,month,day)
                        return "VALID"
                    except :
                        return "INVALID"
                else :
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
	col=data.map(lambda x : (x[1],is_valid_date(x[1])));
	#count "Valid" "Invalid" or "NULL"
	count=col.map(lambda x: (x[1],1)).reduceByKey(lambda x,y:x+y).collect();
	cnt=sc.parallelize(count);
	#output the specific column and the counts
	col.saveAsTextFile('NYPD_data_column1.out');
	cnt.saveAsTextFile('NYPD_count_column1.out');

