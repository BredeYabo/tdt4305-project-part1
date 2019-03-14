from pyspark import SparkConf, SparkContext

sc = SparkContext()

# album id, mtv == 5.0
albums = sc.textFile("albums.csv") \
        .map(lambda line: (line.split(",")))
album_mtv5 = albums.map(lambda x: (x[0],float(x[8]))) \
        .filter(lambda (x,y): y == 5.0)

# Finding corrosponding top artists
album_artist = albums.map(lambda x: (x[1],x[0])) # (artist_id, album_id)

# Get artists name: (Artist_id, country)
artist_country = sc.textFile("artists.csv") \
        .map(lambda line: (line.split(","))) \
        .map(lambda x: (x[0],x[1]))
# (Artist_id, (country,Album_id)) => (album_id,country)
artist_name_album = album_artist \
        .join(artist_country) \
        .map(lambda (x,(y1,y2)): (y1,y2))

top_mtv_artists = album_mtv5.join(artist_name_album) \
        .map(lambda  (x, (y1,y2)): (y2,y1)) \
        .distinct() \
        .sortByKey(True)

top_mtv_artists.map(lambda x: "%s\t%s" %(x[0],x[1])) \
        .coalesce(1, shuffle = True) \
        .saveAsTextFile("result_8")
