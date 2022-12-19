from dotenv import load_dotenv
import json
import os
import psycopg2
import socket
import tweepy

class MyStream(tweepy.StreamingClient):
  def __init__(self, *args, **kw):
    super().__init__(*args, **kw)
    self.i = 1

  def on_connect(self):
    print("Tweets are now streaming!")

  def on_data(self, data):
    try:
        #### Tweet ####
        print(f"----------Tweet # {self.i}----------")
        full_tweet = json.loads(data)

        keys = ['text', 'created_at']
        tweet_dict = { k:full_tweet['data'][k] for k in keys }
        tweet_string = json.dumps(tweet_dict)

        #### Database ####
        db_conn = psycopg2.connect(database=DB_NAME,
              user=DB_USER,
              password=DB_PASS,
              host=DB_HOST,
              port=DB_PORT,
              connect_timeout=3)

        db_cur = db_conn.cursor()
        create_tweets = "CREATE TABLE IF NOT EXISTS tweets (id SERIAL, body JSONB);"
        db_cur.execute(create_tweets)
        db_conn.commit()

        db_cur = db_conn.cursor()
        create_hashtags = "CREATE TABLE IF NOT EXISTS hashtags (hashtag VARCHAR(64), time VARCHAR(64), count INT);"
        db_cur.execute(create_hashtags)
        db_conn.commit()

        insert_tweets = "INSERT INTO Tweets (body) VALUES (%s ::jsonb);"
        db_cur.execute(insert_tweets, (tweet_string,))
        db_conn.commit()
        db_cur.close()

        #### Socket ####
        tweet_encoded = (tweet_string + "\n").encode('utf-8')
        print(tweet_encoded)
        s_conn.send(tweet_encoded)
        self.i = self.i + 1
        return True
    except BaseException as e:
        print(f"Error : {str(e)}")
    return True

  def on_error(self, status):
    print(status)
    return True

def send_data(bearer_token):
  twitter_stream = MyStream(bearer_token)
  twitter_stream.filter(tweet_fields=["created_at", "referenced_tweets"],
                        user_fields=["location", "url"],
                        place_fields=["country", "country_code"],
                        expansions=["author_id", "geo.place_id"])

if __name__ == "__main__":
    load_dotenv()
    BEARER_TOKEN = os.getenv('BEARER_TOKEN')

    DB_NAME = os.getenv('DB_NAME')
    DB_USER = os.getenv('DB_USER')
    DB_PASS = os.getenv('DB_PASS')
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')

    S_HOST = "0.0.0.0"
    S_PORT = 9999

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((S_HOST, S_PORT))
    s.listen()
    print(f"Listening on port: {str(S_PORT)}")

    s_conn, s_addr = s.accept()
    print(f"Received request from: + {str(s_addr)}")

    send_data(BEARER_TOKEN)