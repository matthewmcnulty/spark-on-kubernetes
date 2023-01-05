import os
import pyspark.sql.functions as F

from dotenv import load_dotenv
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, MapType, StructType, StructField, StringType, TimestampType

# ================================================== Section 1 ====================================================

def preprocessing(df):
    df = df \
        .withColumn("cleaned_text", F.lower(F.col('text'))) # Changing to lower case

    df = df \
        .withColumn("cleaned_text", F.regexp_replace('cleaned_text', r'(RT @[\w]*:)', ' ')) # Removing twitter retweets

    df = df \
        .withColumn("cleaned_text", F.regexp_replace('cleaned_text', r'(@[A-Za-z0-9_]+)', ' ')) # Removing twitter handles

    df = df \
        .withColumn("cleaned_text", F.regexp_replace('cleaned_text', r'(#[A-Za-z0-9_]+)', ' ')) # Removing twitter hashtags

    df = df \
        .withColumn("cleaned_text", F.regexp_replace('cleaned_text', r'https?://[A-Za-z0-9./]*', ' ')) # Removing url links

    df = df \
        .withColumn("cleaned_text", F.regexp_replace('cleaned_text', r'([^A-Za-z0-9 ])', ' ')) # Removing special characters

    df = df \
        .withColumn("cleaned_text", F.regexp_replace('cleaned_text', r'(\s+[A-Za-z]\s+)', ' ')) # Removing single characters

    df = df \
        .withColumn("cleaned_text", F.regexp_replace('cleaned_text', r'\s+ ', ' ')) # Removing whitespace

    return df

# ================================================== Section 2 ====================================================

def sentiment_detection(text):
    # Quantifying the polarity of the text within a tweet using VADER function
    return SentimentIntensityAnalyzer().polarity_scores(text)

def sentiment_classification(df):
    # Using a user-defined function to use the above VADER function within PySpark
    sentiment_detection_udf = F.udf(sentiment_detection, MapType(StringType(),FloatType(),False))

    # Appending the cleaned text as a column to the PySpark dataframe
    df = df \
        .withColumn("vader", sentiment_detection_udf("cleaned_text"))

    # Appending the polarity scores as columns to the PySpark dataframe
    df = df \
        .withColumn("neg",df.vader["neg"].cast(FloatType())) \
        .withColumn("neu",df.vader["neu"].cast(FloatType())) \
        .withColumn("pos",df.vader["pos"].cast(FloatType())) \
        .withColumn("compound",df.vader["compound"].cast(FloatType()))

    # Categorising the polarity scores as either positive, negative, or neutral and appending it as a column to the PySpark dataframe
    df = df \
        .withColumn("sentiment", F.when(df.compound <= -0.05, "negative").when(df.compound >= 0.05, "positive").otherwise("neutral"))

    df = df \
        .select(["text", "cleaned_text", "created_at", "sentiment", "neg", "neu", "pos", "compound"])

    return df

# ================================================== Section 3 ====================================================

def batch_hashtags(df, epoch_id):
    print("batch_hashtags(df, epoch_id):")

    # Preprocessing and classifying the sentiment of each mini batch
    df = preprocessing(df)
    df = sentiment_classification(df)

    # Connecting to the database with the JDBC driver and appending the data to the tweets table
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

# ================================================== Section 4 ====================================================

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

    # Starting to process the stream in mini batches every ten seconds
    q1 = tweets_df \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(batch_hashtags) \
        .option("checkpointLocation", "./check") \
        .trigger(processingTime='10 seconds') \
        .start() 

    q1.awaitTermination()