from pyspark import SparkConf, SparkContext
import csv
import unicodedata as uni


sc = SparkContext()

lines = sc.textFile("albums.csv")
mapped = lines.map(lambda line: ((line.split(","))))
# Getting albums by total sum sales
genre_by_albumsales = mapped.map(lambda x: (x[3],int(x[6]))).reduceByKey(lambda x, y: x + y)

# Sorting by count, then genre
genre_by_albumsales = genre_by_albumsales.map(lambda (x,y): ((y,x),x)).sortBy(lambda ((x,y),z): x,ascending=False).map(lambda ((x,y),z): (y,x))

genre_by_albumsales.map(lambda (x,y): '%s\t%s' %(x,y)).coalesce(1, shuffle = False).saveAsTextFile("result_5")
