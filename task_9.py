from pyspark import SparkConf, SparkContext

sc = SparkContext()

lines = sc.textFile("albums.csv")
mapped = lines.map(lambda line: (line.split(",")))
# artist_id, mtv, count
album_mtv = mapped.map(lambda x: (x[1],float(x[8])))
album_count = mapped.map(lambda x: (x[1],1))
# artist_id, total_mtv_critic, count
album_mtv = album_mtv.reduceByKey(lambda x,y: x+y)
album_count = album_count.reduceByKey(lambda x,y: x+y) # Could filter out Norway before
album_mtv_count = album_mtv.join(album_count)

artist_lines = sc.textFile("artists.csv")
artist_country = artist_lines.map(lambda line: (line.split(","))).map(lambda x: (x[0],(x[1],x[5].lower()))).filter(lambda (x,(y,z)): z == "norway")

#(artist_id, real_name, Norway) => (u'49396', ((6.5, 2), (u'Cullen F. Bray', u'norway'))) => (real_name, country, avg_mtv_critic)
average_mtv_critic = album_mtv_count.join(artist_country).map(lambda (x,((y1,y2),(z1,z2))): (z1,(z2,(y1/y2)))).sortByKey(True)
average_mtv_critic = average_mtv_critic.map(lambda (x,y): ((y,x),x)).sortBy(lambda ((x,y),y1): x[1],ascending=False).map(lambda ((x,y),y1): (y,x[0],x[1]))

average_mtv_critic.map(lambda x: "%s\t%s\t%s" %(x[0],x[1],x[2])).coalesce(1, shuffle = False).saveAsTextFile("result_9")
