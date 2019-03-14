from pyspark import SparkConf, SparkContext

sc = SparkContext()

# (artist_id, count)
artist_by_album = sc.textFile("albums.csv") \
        .map(lambda line: ((int(line.split(",")[1]),1))) \
        .reduceByKey(lambda x, y: x + y)

# Sort by count then artist id
artist_by_album = artist_by_album.map(lambda (x,y): ((y,x),x)) \
        .sortByKey(ascending=False) \
        .map(lambda ((x,y),y1): (y,x))

artist_by_album.map(lambda x: "%s\t%s" %(x[0],x[1])) \
        .coalesce(1, shuffle = False) \
        .saveAsTextFile("result_4")
