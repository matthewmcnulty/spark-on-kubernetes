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

st.write(""" # Twitter API v2 and Spark Structured Streaming 💥 : #FIFAWorldCup 🏆⚽🏆 """)
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

        top_20_total = '''
                  SELECT hashtag, SUM(count)::bigint AS count
                  FROM hashtags
                  GROUP BY hashtag ORDER BY count DESC LIMIT 20
                  '''

        top_20_total_cols = ['hashtag', 'count']

        bar_chart_df = pg_to_df(db_conn, top_20_total, top_20_total_cols)

        bar_chart = (
          alt.Chart(bar_chart_df)
          .mark_bar()
          .encode(
            alt.X('hashtag:N', axis=alt.Axis(labelAngle=-45)),
            alt.Y('count:Q'),
            alt.Color("hashtag:N"),
            alt.Tooltip(top_20_total_cols),
          )
          .interactive()
        )

        st.markdown(""" ### Top 20 Trending Hashtags Total 📊""")
        st.markdown(""" ### """)
        st.altair_chart(bar_chart, theme=None, use_container_width=True)

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

        top_5_timeline_cols = ['hashtag', 'window', 'count']

        line_chart_df = pg_to_df(db_conn, top_5_timeline, top_5_timeline_cols)

        line_chart = (
          alt.Chart(line_chart_df)
          .mark_line()
          .encode(
            alt.X('window:T', axis=alt.Axis(labelAngle=-45)),
            alt.Y('count:Q'),
            alt.Color("hashtag:N"),
            alt.Tooltip(top_5_timeline_cols),
          )
          .interactive()
        )

        st.markdown(""" ### Top 5 Trending Hashtags Timeline 📈""")
        st.markdown(""" ### """)
        st.altair_chart(line_chart, theme=None, use_container_width=True)

      hashtags_table = '''
                SELECT * FROM hashtags AS t1 ORDER BY t1.hashtag, t1.window
                '''

      hashtags_table_cols = ['hashtag', 'window', 'count']

      hashtags_df = pg_to_df(db_conn, hashtags_table, hashtags_table_cols)

      st.markdown(""" ### Trending Hashtags Snapshot 🔎""")
      st.markdown(""" ### """)
      st.dataframe(hashtags_df, use_container_width=True)

      time.sleep(10)

except BaseException as e:
    print("Error : " + str(e))