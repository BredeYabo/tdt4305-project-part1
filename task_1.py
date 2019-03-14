# Number of distinct genres
from pyspark import SparkConf, SparkContext

sc = SparkContext()
distinct_genres = sc.textFile("albums.csv") \
        .map(lambda line: (line.split(","))[3]) \
        .distinct()

print(distinct_genres.count())

