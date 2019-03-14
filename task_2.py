from pyspark import SparkConf, SparkContext

sc = SparkContext()

oldest_artist = sc.textFile("artists.csv") \
        .map(lambda line: int((line.split(",")[4]))) \
        .reduce(lambda x,y: y if x > y else x)

print(oldest_artist)


