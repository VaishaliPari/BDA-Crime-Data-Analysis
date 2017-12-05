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

#Function to check indicate whether the crime is completed or attempted x[10]
def to_check_cmplt_attmpt_code(x):
    if x is "" or x is " ":
        return "NULL"
    else:
        attmpt_code_list=['COMPLETED',"ATTEMPTED"]
        if x not in attmpt_code_list:
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
	col=data.map(lambda x : (x[10],to_check_cmplt_attmpt_code(x[10])));
	#count "Valid" "Invalid" or "NULL"
	count=col.map(lambda x: (x[1],1)).reduceByKey(lambda x,y:x+y).collect();
	cnt=sc.parallelize(count);
	#output the specific column and the counts
	col.saveAsTextFile('NYPD_data_column10.out');
	cnt.saveAsTextFile('NYPD_count_column10.out');

