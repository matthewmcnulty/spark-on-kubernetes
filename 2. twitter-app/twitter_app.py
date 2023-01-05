import json
import os
import psycopg2
import socket
import tweepy

from dotenv import load_dotenv

# Inheriting the StreamingClient class and customising it
class MyStream(tweepy.StreamingClient):
  def __init__(self, *args, **kw):
    super().__init__(*args, **kw)
    # Initialising a count of received tweets
    self.i = 1

  def on_connect(self):
    print("Tweets are now streaming!")

  def on_data(self, data):
    try:
        # Receiving tweet and selecting fields of interest
        print(f"========== Tweet # {self.i} ==========")
        full_tweet = json.loads(data)

        keys = ['text', 'created_at']
        tweet_dict = { k:full_tweet['data'][k] for k in keys }
        tweet_string = json.dumps(tweet_dict)

        # Connecting to the database
        db_conn = psycopg2.connect(database=os.getenv('DB_NAME'),
              user=os.getenv('DB_USER'),
              password=os.getenv('DB_PASS'),
              host=os.getenv('DB_HOST'),
              port=os.getenv('DB_PORT'),
              connect_timeout=3)

        db_cur = db_conn.cursor()
        # Creating tweets table for sentiment analysis application
        create_tweets = '''CREATE TABLE IF NOT EXISTS tweets (id SERIAL, text TEXT, cleaned_text TEXT, created_at TIMESTAMP, sentiment VARCHAR(64), neg REAL, neu REAL, pos REAL, compound REAL);'''
        db_cur.execute(create_tweets)
        
        # Creating hashtags table for trending hashtags application
        create_hashtags = '''CREATE TABLE IF NOT EXISTS hashtags (id SERIAL, hashtag VARCHAR(64), time TIMESTAMP, count INT)'''
        db_cur.execute(create_hashtags)
        
        db_conn.commit()
        db_cur.close()

        # Sending tweet to the socket
        tweet_encoded = (tweet_string + "\n").encode('utf-8')
        print(tweet_encoded)
        s_conn.send(tweet_encoded)

        # Adding one to count of received tweets
        self.i = self.i + 1
        return True

    except BaseException as e:
        print("Error : " + str(e))
    return True

  def on_error(self, status):
    print(status)
    return True

def send_data(bearer_token):
  twitter_stream = MyStream(bearer_token)
  # Adding fields to the twitter stream that may be of interest
  twitter_stream.filter(tweet_fields=["created_at", "referenced_tweets"],
                        user_fields=["location", "url"],
                        place_fields=["country", "country_code"],
                        expansions=["author_id", "geo.place_id"])

if __name__ == "__main__":
    # Loading the environment variables
    load_dotenv()

    S_HOST = "0.0.0.0"
    S_PORT = 9999

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((S_HOST, S_PORT))

    s.listen()
    print("Listening on port: " + str(S_PORT))

    s_conn, s_addr = s.accept()
    print("Received request from: " + str(s_addr))

    # Starting the twitter stream once connected
    send_data(os.getenv('BEARER_TOKEN'))