#count crime total in each borough for each precinct code
#<count> <borough> <precinct_code>
#col[13] col[14]


from __future__ import print_function

import sys
from csv import reader
from pyspark import SparkContext

from operator import add
from datetime import datetime

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit total_precincts.py <clean_dataset>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x : reader(x))
    counts = lines.map(lambda col : (col[13]+" - "+str(col[14]), 1)).reduceByKey(add).sortBy(lambda x: x[1], False)
    counts = counts.map(lambda x: str(x[1])+'\t'+x[0])
    counts.saveAsTextFile("total_precinct_count.out")
    sc.stop()

