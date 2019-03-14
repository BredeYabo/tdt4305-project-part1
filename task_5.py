from pyspark import SparkConf, SparkContext


sc = SparkContext()

# (genre, number_of_sales)
genre_by_albumsales = sc.textFile("albums.csv") \
        .map(lambda line: ((line.split(",")))) \
        .map(lambda x: (x[3],int(x[6]))) \
        .reduceByKey(lambda x, y: x + y)

# Sorting by count, then genre
genre_by_albumsales = genre_by_albumsales \
        .map(lambda (x,y): ((y,x),x)) \
        .sortBy(lambda ((x,y),z): x,ascending=False) \
        .map(lambda ((x,y),z): (y,x))

genre_by_albumsales \
        .map(lambda (x,y): '%s\t%s' %(x,y)) \
        .coalesce(1, shuffle = False) \
        .saveAsTextFile("result_5")
