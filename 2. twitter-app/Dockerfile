FROM python:3.7.2

EXPOSE 9999
WORKDIR /usr/src/app

ADD twitter-app.py .
ADD .env .

RUN pip install --upgrade pip
RUN pip install tweepy psycopg2-binary python-dotenv

ENV PYTHONUNBUFFERED=1
CMD ["python", "./twitter-app.py"]