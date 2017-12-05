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

#Bounding Box LatLong details for NYC
lat_max=40.917577
lat_min=40.477399

long_max=-73.700009
long_min=-74.25909

#Function to check if lat long values present in NYC's Bounding Box
def is_present_nyc(point):
    if(point.x<lat_min or point.x>lat_max):
      return False
    if(point.y<long_min or point.y> long_max):
      return False
    return True

#Function to check validity of Lat Long values x[23]
def to_check_lat_long(x):
    if x== "" or x== " "or x=="\t":
        return "NULL"
    x=x.strip("'")
    x=x.replace("(","")
    x=x.replace(")","")
    lat,lon=x.split(",")
    lat=lat.strip()
    lon=lon.strip()
    try:
        lat=float(lat)
        lon=float(lon)
        if is_present_nyc(Point(lat,lon)) :
            return "VALID"
        else:
            return "INVALID"
    except :
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
	col=data.map(lambda x : (x[23],to_check_lat_long(x[23])));
	#count "Valid" "Invalid" or "NULL"
	count=col.map(lambda x: (x[1],1)).reduceByKey(lambda x,y:x+y).collect();
	cnt=sc.parallelize(count);
	#output the specific column and the counts
	col.saveAsTextFile('NYPD_data_column23.out');
	cnt.saveAsTextFile('NYPD_count_column23.out');
