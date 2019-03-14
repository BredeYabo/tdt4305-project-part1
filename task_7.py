from pyspark import SparkConf, SparkContext

sc = SparkContext()

# Load result from previous task
task6_lines = sc.textFile("result_6/part-00000")
task6_mapped = task6_lines.map(lambda line: (line.split("\t")))
top_10_rdd = task6_mapped.map(lambda x: (x[0],float(x[1])))

# TASK 7 
# Finding corrosponding top artists (No order)
lines = sc.textFile("albums.csv")
mapped = lines.map(lambda line: (line.split(",")))
album_artist = mapped.map(lambda x: (x[1],x[0])) # (artist_id, album_id)

# Get artists id and country
lines_artists = sc.textFile("artists.csv")
mapped_artists = lines_artists.map(lambda line: (line.split(",")))
tuppled_artists = mapped_artists.map(lambda x: (x[0],x[5])) # (Artist_id, country)

artist_country = album_artist.join(tuppled_artists) # (Artist_id, (country,Album_id))
album_country = artist_country.map(lambda (x,(y1,y2)): (y1,y2)) # (album_id,country)
top_10_album_rating_country = top_10_rdd.join(album_country)

top_10_album_rating_country = top_10_album_rating_country.map(lambda x: "%s\t%s\t%s" %(x[0],x[1][0],x[1][1]))
top_10_album_rating_country.coalesce(1, shuffle = True).saveAsTextFile("result_7")
