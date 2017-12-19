#total number of crimes for each year

from __future__ import print_function

import sys
from csv import reader
from pyspark import SparkContext

from operator import add
from datetime import datetime

def get_date_year(date_str):
    date_year = datetime.strptime(date_str, '%m/%d/%Y')
    return date_year.strftime('%Y')

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: count_year.py <clean_dataset>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x : reader(x)).map(lambda col: col[1])
    counts = lines.map(get_date_year).map(lambda date_year : (date_year, 1)).reduceByKey(add).sortByKey()
    counts = counts.map(lambda col : col[0]+"\t"+str(col[1]))
    counts.saveAsTextFile("each_year_count.out")
    sc.stop()

