from pyspark import SparkConf, SparkContext
# Find number of artist coming from each country and sort them descending.

sc = SparkContext()

# (Country, count)
artist_by_country = sc.textFile("artists.csv") \
        .map(lambda line: (line.split(",")[5], 1)) \
        .reduceByKey(lambda x, y: x + y)

# Sorting by country # Alphabetically
artist_by_country = artist_by_country.sortByKey(True)
# Sorting by count
artist_by_country = artist_by_country.map(lambda (x,y): ((y,x),x)) \
        .sortBy(lambda ((x,y),y1): x,ascending=False) \
        .map(lambda ((x,y),y1): (y,x))

artist_by_country.map(lambda x: "%s\t%s" %(x[0],x[1])) \
        .coalesce(1, shuffle = False) \
        .saveAsTextFile("result_3")

