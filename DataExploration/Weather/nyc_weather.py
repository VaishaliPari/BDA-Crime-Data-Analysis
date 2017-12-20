#filter weather by JFK Station with code 744860

from __future__ import print_function

import sys
from csv import reader
from pyspark import SparkContext

from operator import add
from datetime import datetime

#Function to check validity of JFK station in x[0]
def check_nyc_weather(x):
    if x is "" or x is " ":
        return False
    #check for 744860
    else:
	list_x =["744860"]
	if x not in list_x:
        	return False
    	else :
        	return True

#Function to convert back to csv as clean dataset
def convert_to_csv_line(data):
    for i in range(len(data)):
        if "," in str(data[i]) :
            data[i]='"'+data[i]+'"'
    return ','.join(d for d in data)

#Function to check all the above functions
def all_checker(x):
	if check_nyc_weather(x[0]):
		return True
	else :
		return False

#main function
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit nyc_weather.py <weather_dataset> ", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    header = lines.take(1) #extract header
    lines = lines.filter(lambda x : x!= header)
    lines = lines.mapPartitions(lambda x: reader(x)).map(lambda x: (x,all_checker(x))).filter(lambda x: x[1]==True).map(lambda x: convert_to_csv_line(x[0]))
    #<filtered_weather_dataset>
    lines.saveAsTextFile('nyc_weather_new.csv')

    sc.stop()
