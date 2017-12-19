#count crime total in each borough for each year between 2006-2016
#3rd argument is borough name
# for STATEN ISLAND, SPECIFY "STATEN ISLAND" in quotes

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
    if len(sys.argv) != 3:
        print("Usage: spark-submit total_eachborough_yearly.py <clean_dataset> <borough_name>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    borough = sys.argv[2]
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x : reader(x)).filter(lambda row: borough in row[13]).map(lambda row: row[1])
    counts = lines.map(get_date_year).map(lambda date_year : (date_year, 1)).reduceByKey(add)
    counts = counts.map(lambda row : row[0]+"\t"+str(row[1]))
    counts.saveAsTextFile("total_borough_yearly_STATEN.out")
    sc.stop()

