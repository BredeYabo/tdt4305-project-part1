# Number of distinct genres
from pyspark import SparkConf, SparkContext

sc = SparkContext()

lines = sc.textFile("/home/slaysmajor/ntnu/bigdata/albums.csv")
mapped = lines.map(lambda line: (line.split(","))[3])

filtered = mapped.filter(lambda x: x != '')
distinct = filtered.distinct()

print(distinct.count())

