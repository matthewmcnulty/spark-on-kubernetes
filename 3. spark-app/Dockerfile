FROM spark-py:v3.3.1
WORKDIR /

USER 0

RUN mkdir ${SPARK_HOME}/spark-app
RUN pip3 install python-dotenv vaderSentiment

COPY spark_hashtags.py ${SPARK_HOME}/spark-app
COPY spark_sentiment.py ${SPARK_HOME}/spark-app
COPY .env ${SPARK_HOME}/spark-app
COPY postgresql-42.5.1.jar ${SPARK_HOME}/spark-app

WORKDIR /opt/spark/work-dir
ENTRYPOINT [ "/opt/entrypoint.sh" ]

ARG spark_uid=185
USER ${spark_uid}