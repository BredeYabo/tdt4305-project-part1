from pyspark import SparkConf, SparkContext
import csv
import unicodedata as uni


sc = SparkContext()

lines = sc.textFile("albums.csv")
mapped = lines.map(lambda line: (line.split(",")))
tuppled = mapped.map(lambda x: (x[3],int(x[6])))

# Getting albums by total sum sales
genre_by_albumsales = tuppled.reduceByKey(lambda x, y: x + y)
# Sorting by album
genre_by_albumsales = genre_by_albumsales.sortByKey()
# Sorting by count
genre_by_albumsales = genre_by_albumsales.map(lambda (x,y): (y,x))
genre_by_albumsales = genre_by_albumsales.sortByKey(False)
genre_by_albumsales = genre_by_albumsales.map(lambda (x,y): (y,x))

genre_by_albumsales.map(lambda (x,y): '%s\t%s' %(x,y)).coalesce(1, shuffle = True).saveAsTextFile("result_5")
