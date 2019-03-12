# Number of distinct genres
from pyspark import SparkConf, SparkContext

sc = SparkContext()

lines = sc.textFile("albums.csv")
mapped = lines.map(lambda line: (line.split(","))[3])

distinct = filtered.distinct()

print(distinct.count())

