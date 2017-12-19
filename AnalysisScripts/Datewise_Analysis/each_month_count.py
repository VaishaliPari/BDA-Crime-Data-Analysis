#total number of crimes for each month

from __future__ import print_function

import sys
from csv import reader
from pyspark import SparkContext

from operator import add
from datetime import datetime

def get_date_month(date_str):
    date_month = datetime.strptime(date_str, '%m/%d/%Y')
    return date_month.strftime('%m')


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: count_month.py <clean_dataset> ", file=sys.stderr)
        exit(-1)

    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x : reader(x)).map(lambda col: col[1])
    counts = lines.map(get_date_month).map(lambda date_month : (date_month, 1)).reduceByKey(add).sortByKey()
    counts = counts.map(lambda col : col[0]+"\t"+str(col[1]))
    counts.saveAsTextFile("each_month_count.out")
    sc.stop()

