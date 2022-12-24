from dotenv import load_dotenv
import os
import pandas as pd
import psycopg2
import time

import streamlit as st
import altair as alt

load_dotenv()

def pg_to_df(conn, query, cols):
    cur = conn.cursor()
    try:
        cur.execute(query)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as e:
        print(f"Error : {str(e)}")
        cur.close()
    
    result = cur.fetchall()
    cur.close()
    
    df = pd.DataFrame(result, columns=cols)
    return df

st.set_page_config(
    page_title="Spark Structured Streaming",
    page_icon="💥",
    layout="wide",
)

st.write(""" # Twitter API v2 and Spark Structured Streaming 💥 : Christmas 🎄🎁🎄 """)
st.markdown(""" # """)

placeholder = st.empty()

try:
  while True:
    db_conn = psycopg2.connect(database=os.getenv('DB_NAME'),
          user=os.getenv('DB_USER'),
          password=os.getenv('DB_PASS'),
          host=os.getenv('DB_HOST'),
          port=os.getenv('DB_PORT'),
          connect_timeout=3)
    
    with placeholder.container():

      fig_col1, fig_col2 = st.columns(2)
      
      with fig_col1:

        percent_query = '''
                  WITH t1 as (
                    SELECT sentiment, COUNT(*) AS n
                    FROM tweets 
                    GROUP BY sentiment
                    )

                  SELECT sentiment, n, ROUND((n)/(SUM(n) OVER ()) * 100, 2)::real AS "percentage"
                  FROM t1;
                  '''

        percent_cols = ['sentiment', 'n', 'percentage']

        percent_df = pg_to_df(db_conn, percent_query, percent_cols)

        percent_chart = (
          alt.Chart(percent_df)
          .mark_arc(innerRadius=125)
          .encode(
            alt.Theta('percentage:Q'),
            alt.Color("sentiment:N"),
            alt.Tooltip(['sentiment', 'n', 'percentage']),
          )
          .interactive()
        )

        st.markdown(""" ### Sentiment Percentages 📊""")
        st.markdown(""" ### """)
        st.altair_chart(percent_chart, theme=None, use_container_width=True)

      with fig_col2:

        count_query = '''
                  WITH t1 as (
                    SELECT *, date_trunc('minute', created_at) AS truncated_created_at
                    FROM tweets
                    )

                  SELECT sentiment, COUNT(*) AS n, truncated_created_at
                  FROM t1
                  GROUP BY truncated_created_at, sentiment
                  ORDER BY truncated_created_at, sentiment;
                  '''

        count_cols = ['sentiment', 'n', 'truncated_created_at']

        count_df = pg_to_df(db_conn, count_query, count_cols)

        count_chart = (
          alt.Chart(count_df)
          .mark_line()
          .encode(
            alt.X('truncated_created_at:T', axis=alt.Axis(labelAngle=-45)),
            alt.Y('n:Q'),
            alt.Color("sentiment:N"),
            alt.Tooltip(count_cols),
          )
          .interactive()
        )

        st.markdown(""" ### Sentiment Timeline 📈""")
        st.markdown(""" ### """)

        st.altair_chart(count_chart, theme=None, use_container_width=True)

      negative_tweets = '''
                      SELECT text, created_at, sentiment, compound FROM tweets WHERE sentiment = 'negative' ORDER BY created_at DESC;;
                      '''
      neutral_tweets = '''
                      SELECT text, created_at, sentiment, compound FROM tweets WHERE sentiment = 'neutral' ORDER BY created_at DESC;;
                      '''
      positive_tweets = '''
                      SELECT text, created_at, sentiment, compound FROM tweets WHERE sentiment = 'positive' ORDER BY created_at DESC;;
                      '''

      tweets_cols = ['text', 'created_at', 'sentiment', 'compound']

      negative_df = pg_to_df(db_conn, negative_tweets, tweets_cols)
      neutral_df = pg_to_df(db_conn, neutral_tweets, tweets_cols)
      positive_df = pg_to_df(db_conn, positive_tweets, tweets_cols)

      st.markdown(""" ### Recent Tweets 🔎""")
      st.markdown(""" ### """)
      tab1, tab2, tab3 = st.tabs(["Positive Tweets", "Neutral Tweets", "Negative Tweets"])

      with tab1:
        st.dataframe(positive_df, use_container_width=True)

      with tab2:
        st.dataframe(neutral_df, use_container_width=True)

      with tab3:
        st.dataframe(negative_df, use_container_width=True)

      time.sleep(10)

except BaseException as e:
    print("Error : " + str(e))