from pyspark import SparkConf, SparkContext
# Find number of artist coming from each country and sort them descending.

sc = SparkContext()

lines = sc.textFile("artists.csv")
artist_mapped = lines.map(lambda line: (line.split(",")[5], 1)) # (Country, 1)

# Getting artist count by country
artist_by_country = artist_mapped.reduceByKey(lambda x, y: x + y)
# Sorting by country # Alphabetically
artist_by_country = artist_by_country.sortByKey(True)
# Sorting by count
artist_by_country = artist_by_country.map(lambda (x,y): ((y,x),x))
artist_by_country = artist_by_country.sortBy(lambda ((x,y),y1): x,ascending=False)
artist_by_country = artist_by_country.map(lambda ((x,y),y1): (y,x))

artist_by_country.map(lambda x: "%s\t%s" %(x[0],x[1])).coalesce(1, shuffle = False).saveAsTextFile("result_3")

