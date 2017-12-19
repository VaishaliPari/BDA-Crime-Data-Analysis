#Locations of crime for each borough
#<borough> <latitude> <longitude>
#col[13] col[21] col[22]

#!/usr/bin/env python
# encoding: utf-8

from __future__ import print_function

import sys
from csv import reader
from operator import add
from pyspark import SparkContext

borough = sys.argv[2]

def get_location(col):
    return col[13] + '\t' + col[21] + '\t' + col[22]

def find_borough(col):
    return borough.upper() in col[13] and col[21] != 'UNDEFINED' and col[22] != 'UNDEFINED'

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: borough_locations.py <clean_dataset> <borough_name>", file=sys.stderr)
        exit(-1)

    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x : reader(x))
    lines = lines.filter(find_borough).map(get_location)
   
    lines.saveAsTextFile("each_borough_locations_STATEN.out")
    sc.stop()
