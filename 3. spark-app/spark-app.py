from dotenv import load_dotenv
import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

load_dotenv()
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

spark = SparkSession \
    .builder \
    .appName("SparkStreaming") \
    .getOrCreate()

schema = StructType([ 
    StructField("text", StringType(), True),
    StructField("created_at" , TimestampType(), True)
    ])

# ================================================== 1 ====================================================

tweets_df = spark \
    .readStream \
    .format("socket") \
    .option("host", "twitter-service") \
    .option("port", 9999) \
    .load() \
    .select(F.from_json(F.col("value").cast("string"), schema).alias("tmp")).select("tmp.*")

# ================================================== 2 ====================================================

df1 = tweets_df \
    .withColumn("hashtags", F.expr("regexp_extract_all(text, r'#(\w+)', 0)")) \
    .select(["hashtags", "created_at"])

df2 = df1 \
    .select(F.explode(df1.hashtags).alias("hashtag"), df1.created_at)

df3 = df2 \
    .groupBy(df2.hashtag, F.window(df2.created_at, "1 minutes")) \
    .count() \
    .orderBy(F.desc("count"))

# ================================================== 3 ====================================================

def func2(df):
    df1 = df.toPandas()

    windows = ([row[0]['start'].strftime('%Y-%m-%d %H:%M:%S') for row in df.select('window').collect()])
    windows = pd.Series(windows, dtype=pd.StringDtype())

    df1['hashtag'] = df1['hashtag'].astype('string')
    df1['window'] = windows.astype('string')
    df1['count'] = df1['count'].astype('int')

    return df1

# ================================================== 4 ====================================================

def batch_hashtags(df, epoch_id):
    print("..in batch_hashtags")

    url_object = URL.create(
        "postgresql",
        username=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
    )

    engine = create_engine(url_object)
    engine_conn = engine.connect()

    df4 = func2(df)
    df4.to_sql('hashtags', engine_conn, if_exists='replace', index=False)
    print("..finished batch_hashtags")

# ================================================== 5 ====================================================

q1 = df3 \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(batch_hashtags) \
    .start()

q1.awaitTermination()