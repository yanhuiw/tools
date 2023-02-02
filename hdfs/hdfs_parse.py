#!usr/bin/python
#encoding: utf-8

import os
import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

def hdfs_parse(file_path):
  # start spark
  conf = SparkConf().setAppName('hdfs_parse')
  conf.set("spark.executor.heartbeatInterval", "60s")
  spark = SparkContext(conf=conf)
  lines = spark.sequenceFile(file_path)
  lists = lines.collect()
  for line in lists:
    # key = line[0]
    value = line[1]
    convert_str = value.decode('utf-8')
    print(convert_str)
  # stop spark
  spark.stop()

if __name__ == '__main__':
  file_path = sys.argv[1]
  hdfs_parse(file_path)
  os._exit(0)