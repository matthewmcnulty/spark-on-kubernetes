import os
import pyspark.sql.functions as F

from dotenv import load_dotenv
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, MapType, StructType, StructField, StringType, TimestampType

# ================================================== 1 ====================================================

def preprocessing(df):
    df = df \
        .withColumn("cleaned_text", F.lower(F.col('text'))) # lower case

    df = df \
        .withColumn("cleaned_text", F.regexp_replace('cleaned_text', r'(RT @[\w]*:)', ' ')) # remove twitter retweets

    df = df \
        .withColumn("cleaned_text", F.regexp_replace('cleaned_text', r'(@[A-Za-z0-9_]+)', ' ')) # remove twitter handles

    df = df \
        .withColumn("cleaned_text", F.regexp_replace('cleaned_text', r'(#[A-Za-z0-9_]+)', ' ')) # remove twitter hashtags

    df = df \
        .withColumn("cleaned_text", F.regexp_replace('cleaned_text', r'https?://[A-Za-z0-9./]*', ' ')) # remove url links

    df = df \
        .withColumn("cleaned_text", F.regexp_replace('cleaned_text', r'([^A-Za-z0-9 ])', ' ')) # remove special characters

    df = df \
        .withColumn("cleaned_text", F.regexp_replace('cleaned_text', r'(\s+[A-Za-z]\s+)', ' ')) # remove single characters

    df = df \
        .withColumn("cleaned_text", F.regexp_replace('cleaned_text', r'\s+ ', ' ')) # remove whitespace

    return df

# ================================================== 2 ====================================================

def sentiment_detection(text):
    return SentimentIntensityAnalyzer().polarity_scores(text)

def sentiment_classification(df):
    # sentiment detection
    sentiment_detection_udf = F.udf(sentiment_detection, MapType(StringType(),FloatType(),False))

    df = df \
        .withColumn("vader", sentiment_detection_udf("cleaned_text"))

    df = df \
        .withColumn("neg",df.vader["neg"].cast(FloatType())) \
        .withColumn("neu",df.vader["neu"].cast(FloatType())) \
        .withColumn("pos",df.vader["pos"].cast(FloatType())) \
        .withColumn("compound",df.vader["compound"].cast(FloatType()))

    df = df \
        .withColumn("sentiment", F.when(df.compound <= -0.05, "negative").when(df.compound >= 0.05, "positive").otherwise("neutral"))

    df = df \
        .select(["text", "cleaned_text", "created_at", "sentiment", "neg", "neu", "pos", "compound"])

    return df

# ================================================== 3 ====================================================

def batch_hashtags(df, epoch_id):
    print("batch_hashtags(df, epoch_id):")

    df = preprocessing(df)
    df = sentiment_classification(df)

    df \
        .write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}") \
        .option("dbtable", "tweets") \
        .option("user", os.getenv('DB_USER')) \
        .option("password", os.getenv('DB_PASS')) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# ================================================== 4 ====================================================

if __name__ == "__main__":
    load_dotenv()

    spark = SparkSession \
        .builder \
        .appName("SparkStreaming") \
        .getOrCreate()

    schema = StructType([ 
        StructField("text", StringType(), True),
        StructField("created_at" , TimestampType(), True)
        ])

    tweets_df = spark \
        .readStream \
        .format("socket") \
        .option("host", "twitter-service") \
        .option("port", 9999) \
        .load() \
        .select(F.from_json(F.col("value").cast("string"), schema).alias("tmp")).select("tmp.*")

    q1 = tweets_df \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(batch_hashtags) \
        .option("checkpointLocation", "./check") \
        .trigger(processingTime='10 seconds') \
        .start() 

    q1.awaitTermination()