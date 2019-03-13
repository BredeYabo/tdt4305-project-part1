from pyspark import SparkConf, SparkContext
import csv
import unicodedata as uni

sc = SparkContext()

lines = sc.textFile("albums.csv")
mapped = lines.map(lambda line: ((int(line.split(",")[1]),1))) # (artist_id, 1)

artist_by_album = mapped.reduceByKey(lambda x, y: x + y) # (artist_id, count)


# Getting artist count by contry
artist_by_album = artist_by_album.sortByKey(ascending=False)
# Sorting by album
# Sorting by count
artist_by_album = artist_by_album.map(lambda (x,y): ((y,x),x)) # (count, artist_id)
artist_by_album = artist_by_album.sortByKey(ascending=False)
artist_by_album = artist_by_album.map(lambda ((x,y),y1): (y,x)) # (artist_id, count)


artist_by_album.map(lambda x: "%s\t%s" %(x[0],x[1])).coalesce(1, shuffle = False).saveAsTextFile("result_4")
