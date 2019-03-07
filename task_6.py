from pyspark import SparkConf, SparkContext
import csv
import unicodedata as uni

sc = SparkContext()

lines = sc.textFile("/home/slaysmajor/ntnu/bigdata/albums.csv")
mapped = lines.map(lambda line: (line.split(",")))
# album id, (rolling, mtv, maniac)
tuppled = mapped.map(lambda x: (x[0],(float(x[7]),float(x[8]),float(x[9]))))

# Getting album by average rating.
album_by_rating = tuppled.mapValues(lambda (x1,x2,x3): (x1+x2+x3)/3.0)
# Sorting by album
album_by_rating = album_by_rating.sortByKey()
# Sorting by average rating
album_by_rating = album_by_rating.map(lambda (x,y): (y,x))
album_by_rating = album_by_rating.sortByKey(False)
album_by_rating = album_by_rating.map(lambda (x,y): (y,x))

# albums = album_by_rating.collect()
# Get top 10 rdd
top_10_rdd = sc.parallelize(album_by_rating.take(10)) # (album_id, avg(rating))


top_10_rdd = top_10_rdd.map(lambda x: "%s\t%s" %(x[0],x[1]))
top_10_rdd.coalesce(1, shuffle = True).saveAsTextFile("result_6")
