from __future__ import print_function
from pyspark import SparkContext
from csv import reader

import sys

import re
import datetime


#Function to check validity Bronx x[1]
def is_valid_borough_name(x):
    if x is "" or x is " ":
        return False
    else:
        borough_names_list=["New York County"]
        if x not in borough_names_list:
            return False
        else :
            return True

#Function to check validity of year for x[2]
def is_valid_date(x):
	if x is "" or x is " ":
		return False
  	#year specifically between 2006 and 2016, others are invalid
  	years_list=["2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016"]
  	if x not in years_list:
      		return False
  	else:
      		return True
            

#Function to check all the above functions
def all_checker(x):
	if is_valid_borough_name(x[1]) and is_valid_date(x[2]):
		return True
	else:
		return False

#Function to convert back to csv as clean dataset
def convert_to_csv_line(data):
    for i in range(len(data)):
        if "," in str(data[i]) :
            data[i]='"'+data[i]+'"'
    return ','.join(d for d in data)

#main function
if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    header = lines.take(1) #extract header
    lines = lines.filter(lambda x : x!= header)
    lines = lines.mapPartitions(lambda x: reader(x)).map(lambda x: (x,all_checker(x))).filter(lambda x: x[1]==True).map(lambda x: convert_to_csv_line(x[0]))

    lines.saveAsTextFile('population_Manhattan_county.out')

