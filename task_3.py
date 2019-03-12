from pyspark import SparkConf, SparkContext

sc = SparkContext()

lines = sc.textFile("artists.csv")
mapped = lines.map(lambda line: (line.split(",")))
tuppled = mapped.map(lambda x: (x[5],1))

# Getting artist count by contry
artist_by_country = tuppled.reduceByKey(lambda x, y: x + y)
# Sorting by country # Alphabetically
artist_by_country = artist_by_country.sortByKey(True)
# Sorting by count
artist_by_country = artist_by_country.map(lambda (x,y): (y,x))
artist_by_country = artist_by_country.sortByKey(False)
artist_by_country = artist_by_country.map(lambda (x,y): (y,x))

artist_by_country.map(lambda x: "%s\t%s" %(x[0],x[1])).coalesce(1, shuffle = True).saveAsTextFile("result_3")

