from pyspark import SparkConf, SparkContext
import csv
import unicodedata as uni


sc = SparkContext()

lines = sc.textFile("/home/slaysmajor/ntnu/bigdata/albums.csv")
mapped = lines.map(lambda line: (line.split(",")))
tuppled = mapped.map(lambda x: (int(x[1]),1)) # (album_id, count)

# Getting artist count by contry
artist_by_album = tuppled.reduceByKey(lambda x, y: x + y) # (album_id, count)
# Sorting by album
artist_by_album = artist_by_album.sortByKey(ascending=False)
# Sorting by count
artist_by_album = artist_by_album.map(lambda (x,y): (y,x)) # (count, album_id)
artist_by_album = artist_by_album.sortByKey(ascending=False)
artist_by_album = artist_by_album.map(lambda (x,y): (y,x)) # (album_id, count)


artist_by_album.map(lambda x: "%s\t%s" %(x[0],x[1])).coalesce(1, shuffle = True).saveAsTextFile("result_4")
