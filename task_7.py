from pyspark import SparkConf, SparkContext

sc = SparkContext()

# Load result from previous task
top_10_rdd = sc.textFile("result_6/part-00000") \
        .map(lambda line: (line.split("\t"))) \
        .map(lambda x: (x[0],float(x[1])))
# TASK 7 
# Finding corrosponding top artists (No order)
album_artist = sc.textFile("albums.csv") \
        .map(lambda line: (line.split(","))) \
        .map(lambda x: (x[1],x[0])) # (artist_id, album_id)
# Get artists id and country
artist_country = sc.textFile("artists.csv") \
        .map(lambda line: (line.split(","))) \
        .map(lambda x: (x[0],x[5])) # (Artist_id, country)
# (Artist_id, (country,Album_id)) => # (album_id,country)
album_country = album_artist.join(artist_country) \
        .map(lambda (x,(y1,y2)): (y1,y2))
top_10_album_rating_country = top_10_rdd.join(album_country)

top_10_album_rating_country.map(lambda x: "%s\t%s\t%s" %(x[0],x[1][0],x[1][1])) \
        .coalesce(1, shuffle = True).saveAsTextFile("result_7")
