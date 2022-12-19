FROM python:3.7.2

EXPOSE 9999
WORKDIR /usr/src/app

ADD streamlit-app.py .
ADD .env .

RUN pip install --upgrade pip
RUN pip install pandas psycopg2-binary python-dotenv streamlit altair

ENV PYTHONUNBUFFERED=1
CMD ["streamlit", "run", "./streamlit-app.py", "--server.port", "80"]