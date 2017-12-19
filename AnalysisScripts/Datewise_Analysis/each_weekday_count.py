#total number of crimes for each weekday

from __future__ import print_function

import sys
from csv import reader
from pyspark import SparkContext

from operator import add
from datetime import datetime

def get_weekday(date_str):
    date_weekday = datetime.strptime(date_str, '%m/%d/%Y')
    return date_weekday.weekday()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: count_weekday.py <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x : reader(x)).map(lambda col: col[1])
    counts = lines.map(get_weekday).map(lambda date_weekday : (date_weekday, 1)).reduceByKey(add)
    counts = counts.map(lambda col : str(col[0])+"\t"+str(col[1]))
    counts.saveAsTextFile("each_weekday_count.out")
    sc.stop()

