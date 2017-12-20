#Average of Temperature Humidity and windspeed on Days of the months

from __future__ import print_function

import sys
from csv import reader
from operator import add

from pyspark import SparkContext
from datetime import datetime

def fetch_date(row):
    date1=row[0]
    day1 = date1[7:9]
    day = day1[0:2]
    date_row = day

    temperature = 0 if row[1][0] == '' else float(row[1][0])
    humidity = 0 if row[1][1] == '' else float(row[1][1])
    wind_speed = 0 if row[1][2] == '' else float(row[1][2])
    return (date_row), (temperature, humidity, wind_speed, row[1][3])

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: avg_weather_days.py <filtered_weather_dataset>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x : reader(x)).map(lambda row: (row[2], (row[3], row[5], row[13], 1)))
    counts = lines.map(fetch_date).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1], x[2]+y[2], x[3]+y[3]))
    counts = counts.map(lambda row : row[0]+", "+"%.2f, %.2f, %.2f" % (row[1][0]/row[1][3], row[1][1]/row[1][3], row[1][2]/row[1][3]))
    counts.saveAsTextFile("weather_average_days.out")
    sc.stop()
