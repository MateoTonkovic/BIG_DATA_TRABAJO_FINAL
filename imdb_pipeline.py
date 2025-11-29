from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    split, explode, col, coalesce, lit, avg, count, floor
)
from pyspark.sql.types import IntegerType

# Crear sesión Spark
spark = SparkSession.builder \
    .appName("IMDB Pipeline Productora") \
    .master("spark://spark-master:7077") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.cores.max", "4") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# Rutas
hdfs = "hdfs://namenode:9000"
landing = hdfs + "/landing/imdb"
raw_out = hdfs + "/raw/imdb"
curated = hdfs + "/curated/imdb"

opts = {"sep": "\t", "header": "true", "nullValue": "\\N"}

# Leer archivos TSV
title_basics = spark.read.options(**opts).csv(landing + "/title.basics.tsv")
title_ratings = spark.read.options(**opts).csv(landing + "/title.ratings.tsv")
name_basics = spark.read.options(**opts).csv(landing + "/name.basics.tsv")

# Transformaciones básicas
title_basics = title_basics \
    .withColumn("startYear", col("startYear").cast(IntegerType())) \
    .withColumn("endYear", col("endYear").cast(IntegerType())) \
    .withColumn("runtimeMinutes", col("runtimeMinutes").cast(IntegerType())) \
    .withColumn("genres_array", split(col("genres"), ","))

title_ratings = title_ratings \
    .withColumn("averageRating", col("averageRating").cast("double")) \
    .withColumn("numVotes", col("numVotes").cast("long"))

name_basics = name_basics.withColumn(
    "knownForArray", split(col("knownForTitles"), ",")
)

# Guardar en RAW
title_basics.write.mode("overwrite").parquet(raw_out + "/title_basics")
title_ratings.write.mode("overwrite").parquet(raw_out + "/title_ratings")
name_basics.write.mode("overwrite").parquet(raw_out + "/name_basics")

# title_with_ratings_nopartition
title_wr = title_basics.join(title_ratings, "tconst", "left")

title_wr.write.mode("overwrite").parquet(curated + "/title_with_ratings_nopartition")

# title_by_genre
titles_by_genre = title_wr.withColumn("genre", explode(col("genres_array")))

titles_by_genre.write.mode("overwrite").parquet(curated + "/title_by_genre")

# genre_stats
genre_stats = titles_by_genre.groupBy("genre").agg(
    count("*").alias("title_count"),
    avg("averageRating").alias("avg_rating"),
    avg("runtimeMinutes").alias("avg_runtime"),
    avg("numVotes").alias("avg_votes")
)

genre_stats.write.mode("overwrite").parquet(curated + "/genre_stats")

# title_popularity
popularity = title_wr.select(
    "tconst", "primaryTitle", "titleType",
    "averageRating", "numVotes"
).withColumn(
    "popularity_score",
    (col("numVotes") * col("averageRating"))
)

popularity.write.mode("overwrite").parquet(curated + "/title_popularity")

# person_rating_summary
known = name_basics.withColumn("tconst", explode("knownForArray"))
person_rating = known.join(title_ratings, "tconst", "left") \
    .groupBy("nconst", "primaryName", "primaryProfession") \
    .agg(
        avg("averageRating").alias("avg_rating"),
        count("tconst").alias("title_count")
    )

person_rating.write.mode("overwrite").parquet(curated + "/person_rating_summary")

# title_by_decade
title_wr = title_wr.withColumn(
    "decade",
    floor(col("startYear") / 10) * 10
)

title_wr.write.mode("overwrite").parquet(curated + "/title_by_decade")

spark.stop()