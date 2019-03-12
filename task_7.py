from pyspark import SparkConf, SparkContext
import csv
import unicodedata as uni

sc = SparkContext()

lines = sc.textFile("albums.csv")
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
# top_10_rdd = sc.parallelize(album_by_rating.take(10)) # (album_id, avg(rating))
top_10_rdd = album_by_rating.zipWithIndex().filter(lambda x: x[1] < 10).keys()
# Returns an RDD, more efficient than parallelizing. The downside is that it shuffles the result, it's not ordered anymore. The task doesn't specify a order

# TASK 7
# Finding corrosponding top artists
album_artist = mapped.map(lambda x: (x[1],x[0])) # (artist_id, album_id)
# top_10_artists_id = top_10_rdd.join(tuppled2)
# print(top_10_artists_id.collect())
# Get artists id and country
lines_artists = sc.textFile("artists.csv")
mapped_artists = lines_artists.map(lambda line: (line.split(",")))
tuppled_artists = mapped_artists.map(lambda x: (x[0],x[5])) # (Artist_id, country)

artist_country = album_artist.join(tuppled_artists) # (Artist_id, (country,Album_id))
album_country = artist_country.map(lambda (x,(y1,y2)): (y1,y2)) # (album_id,country)
top_10_album_rating_country = top_10_rdd.join(album_country)

top_10_album_rating_country = top_10_album_rating_country.map(lambda x: "%s\t%s\t%s" %(x[0],x[1][0],x[1][1]))
top_10_album_rating_country.coalesce(1, shuffle = True).saveAsTextFile("result_7")

# Using Coalesce because it dosn't shuffle the partitions and we only need one partition when writing out the data. We are also always reducing the number of partitions and we won't suffer from uneven partitions since we will only have one. This increases performance when compared to repartition.
