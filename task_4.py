from pyspark import SparkConf, SparkContext
import csv
import unicodedata as uni

sc = SparkContext()

lines = sc.textFile("albums.csv")
# (artist_id, count)
artist_by_album = lines.map(lambda line: ((int(line.split(",")[1]),1))).reduceByKey(lambda x, y: x + y)# (artist_id, 1)

# Sort by count then artist id
artist_by_album = artist_by_album.map(lambda (x,y): ((y,x),x)).sortByKey(ascending=False).map(lambda ((x,y),y1): (y,x))

artist_by_album.map(lambda x: "%s\t%s" %(x[0],x[1])).coalesce(1, shuffle = False).saveAsTextFile("result_4")
