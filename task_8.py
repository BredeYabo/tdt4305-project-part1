from pyspark import SparkConf, SparkContext
import csv
import unicodedata as uni

sc = SparkContext()

lines = sc.textFile("/home/slaysmajor/ntnu/bigdata/albums.csv")
mapped = lines.map(lambda line: (line.split(",")))
# album id, mtv
tuppled = mapped.map(lambda x: (x[0],float(x[8])))

# Getting album by average rating.
# Sorting by album
album_by_rating = album_by_rating.sortByKey()
# Sorting by average rating
album_by_rating = album_by_rating.map(lambda (x,y): (y,x))
album_by_rating = album_by_rating.filter(lambda (x,y): x == 5.0) # (album_id, count)
album_by_rating = album_by_rating.sortByKey(False)
album_by_rating = album_by_rating.map(lambda (x,y): (y,x))
print(album_rating.take(10))

# albums = album_by_rating.collect()
# Get top 10 rdd
top_10_rdd = sc.parallelize(album_by_rating.take(10)) # (album_id, avg(rating))


# Finding corrosponding top artists
album_artist = mapped.map(lambda x: (x[1],x[0])) # (artist_id, album_id)
# top_10_artists_id = top_10_rdd.join(tuppled2)
# print(top_10_artists_id.collect())
# Get artists id and country
lines_artists = sc.textFile("/home/slaysmajor/ntnu/bigdata/artists.csv")
mapped_artists = lines_artists.map(lambda line: (line.split(",")))
tuppled_artists = mapped_artists.map(lambda x: (x[0],x[5])) # (Artist_id, country)

artist_country = album_artist.join(tuppled_artists) # (Artist_id, (country,Album_id))
album_country = artist_country.map(lambda (x,(y1,y2)): (y1,y2)) # (album_id,country)
top_10_album_rating_country = top_10_rdd.join(album_country)

top_10_album_rating_country = top_10_album_rating_country.map(lambda x: "%s\t%s\t%s" %(x[0],x[1][0],x[1][1]))
top_10_album_rating_country.coalesce(1, shuffle = True).saveAsTextFile("result_7")



# top_10_rdd = top_10_rdd.map(lambda x: "%s\t%s" %(x[0],x[1]))
# top_10_rdd.coalesce(1, shuffle = True).saveAsTextFile("result_6")
