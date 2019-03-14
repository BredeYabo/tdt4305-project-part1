from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

artistSchema = StructType([
    StructField("artist_id", IntegerType(), True),
    StructField("real_name", StringType(), True),
    StructField("art_name", StringType(), True),
    StructField("role", StringType(), True),
    StructField("year_of_birth", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("email", StringType(), True),
    StructField("zip_code", IntegerType(), True)])

albumSchema = StructType([
    StructField("album_id", IntegerType(), True),
    StructField("artis_id", StringType(), True),
    StructField("album_title", StringType(), True),
    StructField("genre", StringType(), True),
    StructField("year_of_pub", IntegerType(), True),
    StructField("num_of_tracks", StringType(), True),
    StructField("num_of_sales", StringType(), True),
    StructField("rolling_stone_critic", StringType(), True),
    StructField("mtv_critic", StringType(), True),
    StructField("music_maniac_critic", IntegerType(), True)])


artistsDF = spark.read.csv("artists.csv",schema=artistSchema)
artistsDF.createOrReplaceTempView("artists")
albumsDF = spark.read.csv("albums.csv",schema=albumSchema)
albumsDF.createOrReplaceTempView("albums")
# a) Number of distinct artists
spark.sql('select count(distinct(artist_id)) from artists').show()
# b) Number of Distinct albums
spark.sql('select count(distinct(album_id)) from albums').show()
# c) Number of disting genres
spark.sql('select count(distinct(genre)) from albums').show()
# d) Number of distinct countries
spark.sql('select count(distinct(country)) from artists').show()
# e) Minimum Year_of_pub
spark.sql('select min(year_of_pub) from albums').show()
# f) Maximum year_of_pub
spark.sql('select max(year_of_pub) from albums').show()
# g) Minimum year_of_birth
spark.sql('select min(year_of_birth) from artists').show()
# h) Maximum year_of_birth
spark.sql('select max(year_of_birth) from artists').show()

