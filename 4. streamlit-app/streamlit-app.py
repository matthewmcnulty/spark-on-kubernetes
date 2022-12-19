from dotenv import load_dotenv
import os
import pandas as pd
import psycopg2
import time

import streamlit as st
import altair as alt

load_dotenv()
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

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

st.write("""

# Trending Twitter Hashtags using Spark Structured Streaming: #FIFAWorldCup 🏆⚽🏆

""")

placeholder = st.empty()

try:
  while True:
    db_conn = psycopg2.connect(database=DB_NAME,
          user=DB_USER,
          password=DB_PASS,
          host=DB_HOST,
          port=DB_PORT,
          connect_timeout=3)
    
    with placeholder.container():

      fig_col1, fig_col2 = st.columns(2)
      
      with fig_col1:

        top_20_total = '''
                  SELECT hashtag, SUM(count)::bigint AS count
                  FROM hashtags
                  GROUP BY hashtag ORDER BY count DESC LIMIT 20
                  '''

        db_cols1 = ['hashtag', 'count']

        bar_chart_df = pg_to_df(db_conn, top_20_total, db_cols1)

        bar_chart = (
          alt.Chart(bar_chart_df)
          .mark_bar()
          .encode(
            alt.X('hashtag:N', axis=alt.Axis(labelAngle=-45)),
            alt.Y('count:Q'),
            alt.Color("hashtag:N"),
            alt.Tooltip(['hashtag', 'count']),
          )
          .interactive()
        )

        st.markdown(""" ### Top 20 Trending Hashtags Total 📊""")
        st.altair_chart(bar_chart, use_container_width=True)

      with fig_col2:

        top_5_timeline = '''
                  SELECT t1.hashtag, t1.window, t1.count
                  FROM hashtags as t1
                  INNER JOIN (SELECT hashtag, SUM(count) AS count
                        FROM hashtags
                        GROUP BY hashtag ORDER BY count DESC LIMIT 5) AS t2
                        ON t1.hashtag = t2.hashtag
                  ORDER BY t1.hashtag, t1.window
                  '''

        db_cols2 = ['hashtag', 'window', 'count']

        line_chart_df = pg_to_df(db_conn, top_5_timeline, db_cols2)

        line_chart = (
          alt.Chart(line_chart_df)
          .mark_line()
          .encode(
            alt.X('window:T', axis=alt.Axis(labelAngle=-45)),
            alt.Y('count:Q'),
            alt.Color("hashtag:N"),
            alt.Tooltip(['hashtag', 'window', 'count']),
          )
          .interactive()
        )

        st.markdown(""" ### Top 5 Trending Hashtags Timeline 📈""")
        st.altair_chart(line_chart, use_container_width=True)

      hashtags_table = '''
                SELECT * FROM hashtags AS t1 ORDER BY t1.hashtag, t1.window
                '''

      db_cols3 = ['hashtag', 'window', 'count']

      hashtags_df = pg_to_df(db_conn, hashtags_table, db_cols3)

      st.markdown(""" ### Trending Hashtags Snapshot 🔎""")
      st.dataframe(hashtags_df, use_container_width=True)

      time.sleep(5)

except BaseException as e:
    print(f"Error : {str(e)}")