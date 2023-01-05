import os
import pandas as pd
import pyspark.sql.functions as F

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# ================================================== Section 1 ====================================================

def preprocessing(df):
    # Extracting the start time from PySpark row object
    windows = ([row[0]['start'].strftime('%Y-%m-%d %H:%M:%S') for row in df.select('window').collect()])
    windows = pd.Series(windows, dtype=pd.StringDtype())

    # Converting the PySpark df to Pandas df
    df = df.toPandas()
    df['hashtag'] = df['hashtag'].astype('string')
    df['window'] = windows.astype('string')
    df['count'] = df['count'].astype('int')

    return df

# ================================================== Section 2 ====================================================

def batch_hashtags(df, epoch_id):
    print("batch_hashtags(df, epoch_id):")

    # Creating database connection string
    url_object = URL.create(
        "postgresql",
        username=os.getenv('DB_USER'),
        password=os.getenv('DB_PASS'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database=os.getenv('DB_NAME'),
    )

    # Connecting to database with sqlalchemy
    engine = create_engine(url_object)
    engine_conn = engine.connect()

    # Preprocessing each mini batch
    df = preprocessing(df)

    # Updating the data of the hashtags table
    df.to_sql('hashtags', engine_conn, if_exists='replace', index=False)

# ================================================== Section 3 ====================================================

if __name__ == "__main__":
    # Loading the environment variables
    load_dotenv()

    # Creating the SparkSession
    spark = SparkSession \
        .builder \
        .appName("SparkStreaming") \
        .getOrCreate()

    # Defining the schema of the incoming JSON string
    schema = StructType([ 
        StructField("text", StringType(), True),
        StructField("created_at" , TimestampType(), True)
        ])

    # Consuming the data stream from the twitter_app.py socket
    tweets_df = spark \
        .readStream \
        .format("socket") \
        .option("host", "twitter-service") \
        .option("port", 9999) \
        .load() \
        .select(F.from_json(F.col("value").cast("string"), schema).alias("tmp")).select("tmp.*")

    # Getting the hashtags within each tweet
    tweets_df = tweets_df \
        .withColumn("hashtags", F.expr("regexp_extract_all(text, r'#(\w+)', 0)")) \
        .select(["hashtags", "created_at"])

    # Getting the individual hashtags and the time it was created at
    tweets_df = tweets_df \
        .select(F.explode(tweets_df.hashtags).alias("hashtag"), tweets_df.created_at)

    # Counting the total number of times a hashtag appears each minute
    tweets_df = tweets_df \
        .groupBy(tweets_df.hashtag, F.window(tweets_df.created_at, "1 minutes")) \
        .count() \
        .orderBy(F.desc("count"))

    # Starting to process the stream in mini batches every ten seconds
    q1 = tweets_df \
        .writeStream \
        .outputMode("complete") \
        .foreachBatch(batch_hashtags) \
        .option("checkpointLocation", "./check") \
        .trigger(processingTime='10 seconds') \
        .start() 

    q1.awaitTermination()