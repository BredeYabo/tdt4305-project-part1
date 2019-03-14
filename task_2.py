from pyspark import SparkConf, SparkContext

sc = SparkContext()

artists = sc.textFile("artists.csv").map(lambda line: int((line.split(",")[4])))
oldest_artist = artists.reduce(lambda x,y: y if x > y else x)

print(oldest_artist)


