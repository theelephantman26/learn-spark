from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f

spark = SparkSession.builder.master("local[1]").appName("movies").getOrCreate()

movies_schema = t.StructType([
    t.StructField("userId", t.IntegerType(), nullable=False),
    t.StructField("movieId", t.IntegerType(), nullable=False),
    t.StructField("rating", t.FloatType(), nullable=False),
    t.StructField("timestamp", t.LongType(), nullable=False),
])

dataset = spark.read.options(delimeter=",", header=True).csv("movies/ratings.csv", schema=movies_schema)
dataset.show()
dataset = dataset.withColumn("timestamp", f.timestamp_seconds(f.col("timestamp")))
dataset.show()
dataset.printSchema()