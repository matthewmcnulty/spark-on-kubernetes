FROM spark-py:v3.3.1
WORKDIR /

USER 0

RUN mkdir ${SPARK_HOME}/twitter-spark-app
RUN pip3 install pandas sqlalchemy psycopg2-binary python-dotenv 

COPY ./spark-app.py ${SPARK_HOME}/twitter-spark-app
COPY .env ${SPARK_HOME}/twitter-spark-app

WORKDIR /opt/spark/work-dir
ENTRYPOINT [ "/opt/entrypoint.sh" ]

ARG spark_uid=185
USER ${spark_uid}