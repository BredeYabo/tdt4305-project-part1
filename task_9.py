from pyspark import SparkConf, SparkContext
import csv
import unicodedata as uni

sc = SparkContext()

lines = sc.textFile("albums.csv")
mapped = lines.map(lambda line: (line.split(",")))
# artist_id, mtv
album_mtv = mapped.map(lambda x: (x[1],float(x[8])))


artist_lines = sc.textFile("artists.csv")
artist_mapped = lines.map(lambda line: (line.split(",")))
# (artist_id, country)
album_mtv = mapped.map(lambda x: (x[0],x[1],x[5])).filter(lambda (x,y,z): z == "Norway")



artist_mtv = artist_mtv.map(lambda x: "%s\t%s" %(x[0],x[1]))
artist_mtv.coalesce(1, shuffle = True).saveAsTextFile("result_9")

# album_mtv5 = album_mtv5.map(lambda x: "%s\t%s" %(x[0],x[1]))
# album_mtv5.coalesce(1, shuffle = True).saveAsTextFile("result_6")
