from pyspark import SparkConf, SparkContext

sc = SparkContext()

# album id, (avg_mtv_rating)
album_by_rating = sc.textFile("albums.csv") \
        .map(lambda line: (line.split(","))) \
        .map(lambda x: (x[0],(float(x[7]),float(x[8]),float(x[9])))) \
        .mapValues(lambda (x1,x2,x3): (x1+x2+x3)/3.0)

# Sorting by album
album_by_rating = album_by_rating.sortByKey()
# Sorting by average rating
album_by_rating = album_by_rating.map(lambda (x,y): ((y,x),x)) \
        .sortBy(lambda ((x,y),z): x,ascending=False) \
        .map(lambda ((x,y),z): (y,x))

# albums = album_by_rating.collect()
# Get top 10 rdd
top_10_rdd = album_by_rating.zipWithIndex() \
        .filter(lambda x: x[1] < 10).keys()

top_10_rdd.map(lambda x: "%s\t%s" %(x[0],x[1])) \
        .coalesce(1, shuffle = False) \
        .saveAsTextFile("result_6")
