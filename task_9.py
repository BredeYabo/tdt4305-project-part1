from pyspark import SparkConf, SparkContext
import csv
import unicodedata as uni

sc = SparkContext()

lines = sc.textFile("albums.csv")
mapped = lines.map(lambda line: (line.split(",")))
# album id, mtv
tuppled = mapped.map(lambda x: (x[0],float(x[8])))

# Getting mtv rating == 5.0
album_by_rating = tuppled.map(lambda (x,y): (y,x))
album_by_rating = album_by_rating.filter(lambda (x,y): x == 5.0)
album_by_rating = album_by_rating.map(lambda (x,y): (y,x))

# All albums with mtv score 5
album_mtv5 =  album_by_rating # (album_id, mtv_rating)


# Finding corrosponding top artists
album_artist = mapped.map(lambda x: (x[1],x[0])) # (artist_id, album_id)

# Get artists name
lines_artists = sc.textFile("artists.csv")
mapped_artists = lines_artists.map(lambda line: (line.split(",")))
tuppled_artists = mapped_artists.map(lambda x: (x[0],x[1])) # (Artist_id, country)

artist_name = album_artist.join(tuppled_artists) # (Artist_id, (country,Album_id))
artist_name_album = artist_name.map(lambda (x,(y1,y2)): (y1,y2)) # (album_id,country)
top_mtv_artists = album_mtv5.join(artist_name_album)

top_mtv_artists = top_mtv_artists.map(lambda  (x, (y1,y2)): (y2,y1))
top_mtv_artists = top_mtv_artists.distinct()
top_mtv_artists = top_mtv_artists.sortByKey(True)

top_mtv_artists = top_mtv_artists.map(lambda x: "%s\t%s" %(x[0],x[1]))
top_mtv_artists.coalesce(1, shuffle = True).saveAsTextFile("result_9")

# album_mtv5 = album_mtv5.map(lambda x: "%s\t%s" %(x[0],x[1]))
# album_mtv5.coalesce(1, shuffle = True).saveAsTextFile("result_6")
