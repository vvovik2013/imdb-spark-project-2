"""
SparkSession initialization & Schemes of datasets
"""
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
from pyspark.sql.types import ArrayType, StringType

spark_session = (SparkSession.builder
                             .master("local")
                             .appName("task app")
                             .config(conf=SparkConf())
                             .getOrCreate())
shema_akas = t.StructType([
                t.StructField("titleId", t.StringType(), True),
                t.StructField("ordering", t.IntegerType(), True),
                t.StructField("title", t.StringType(), True),
                t.StructField("region", t.StringType(), True),
                t.StructField("language", t.StringType(), True),
                t.StructField("types", t.StringType(), True),
                t.StructField("attributes", t.StringType(), True),
                t.StructField("isOriginalTitle", t.StringType(), True)])

shema_basics = t.StructType([
                t.StructField("tconst", t.StringType(), True),
                t.StructField("titleType", t.StringType(), True),
                t.StructField("primaryTitle", t.StringType(), True),
                t.StructField("originalTitle", t.StringType(), True),
                t.StructField("isAdult", t.IntegerType(), True),
                t.StructField("startYear", t.StringType(), True),
                t.StructField("endYear", t.StringType(), True),
                t.StructField("runtimeMinutes", t.IntegerType(), True),
                t.StructField("genres", StringType(), True)])

shema_crew = t.StructType([
                t.StructField("tconst", t.StringType(), True),
                t.StructField("directors", t.StringType(), True),
                t.StructField("writers", t.StringType(), True)])

shema_episode = t.StructType([
                   t.StructField("tconst", t.StringType(), True),
                   t.StructField("parentTconst", t.StringType(), True),
                   t.StructField("seasonNumber", t.IntegerType(), True),
                   t.StructField("episodeNumber", t.IntegerType(), True)])

shema_principals = t.StructType([
                   t.StructField("tconst", t.StringType(), True),
                   t.StructField("ordering", t.IntegerType(), True),
                   t.StructField("nconst", t.StringType(), True),
                   t.StructField("category", t.StringType(), True),
                   t.StructField("job", t.StringType(), True),
                   t.StructField("characters", t.StringType(), True)])

shema_ratings = t.StructType([
                   t.StructField("tconst", t.StringType(), True),
                   t.StructField("averageRating", t.StringType(), True),
                   t.StructField("numVotes", t.IntegerType(), True)])

shema_name_basics = t.StructType([
                    t.StructField("nconst", t.StringType(), True),
                    t.StructField("primaryName", t.StringType(), True),
                    t.StructField("birthYear", t.IntegerType(), True),
                    t.StructField("deathYear", t.StringType(), True),
                    t.StructField("primaryProfession", t.IntegerType(), True),
                    t.StructField("knownForTitles", t.StringType(), True)])
