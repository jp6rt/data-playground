import os
from datetime import datetime
from pyspark.sql import SparkSession, Row

access_key = ''
secret_key = ''
session_token = ''

spark = SparkSession.builder \
        .appName("s3_connect_test") \
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider") \
        .config("spark.jars", "jars/aws-java-sdk-bundle-1.12.162.jar") \
        .config("spark.jars", "jars/hadoop-aws-3.3.4.jar") \
        .config('fs.s3a.access.key', access_key) \
        .config('fs.s3a.secret.key', secret_key) \
        .config('fs.s3a.session.token', session_token) \
        .getOrCreate()

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-bundle-1.12.162,org.apache.hadoop:hadoop-aws-3.3.4 pyspark-shell'

path_prefix = 's3a://jp6rt-etl-playground/datasets/ml-25m'
movies_df = spark.read.option("header", True).csv(f'{path_prefix}/movies.csv')
ratings_df = spark.read.option("header", True).csv(f'{path_prefix}/ratings.csv')

ratings_df.createOrReplaceTempView('ratings')
movies_df.createOrReplaceTempView('movies')


movie_ratings_df = spark.sql("SELECT m.movieId, m.title, m.genres, r.userId, r.rating, r.timestamp \
FROM movies m INNER JOIN ratings r ON m.movieId = r.movieId")

def enrich_row(row):
    year = datetime.fromtimestamp(int(row.timestamp)).year
    return Row(
        movieId=int(row.movieId),
        title=row.title,
        genres=row.genres,
        userId=int(row.userId),
        rating=float(row.rating),
        timestamp=int(row.timestamp),
        year=year
    )

enriched_movie_ratings = movie_ratings_df.rdd.map(enrich_row)

enriched_movie_ratings_df = spark.createDataFrame(enriched_movie_ratings)
enriched_movie_ratings_df.createOrReplaceTempView('movies_ratings')

movies_ratings_count = spark.sql("SELECT \
                                        movieId, title, avg(rating) as avg_rating, count(rating) as count \
                                        FROM movies_ratings \
                                        GROUP BY movieId, title \
                                        ORDER BY count DESC, avg_rating DESC")

top100_highest_rated_movies = movies_ratings_count.take(100)

print(top100_highest_rated_movies)

