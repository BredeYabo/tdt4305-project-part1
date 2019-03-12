from pyspark import SparkConf, SparkContext

sc = SparkContext()

lines = sc.textFile("artists.csv")
mapped = lines.map(lambda line: int((line.split(",")[4])))
oldest_artist = mapped.reduce(lambda x,y: y if x > y else x)

print(oldest_artist)


