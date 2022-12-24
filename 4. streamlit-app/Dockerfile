FROM python:3.7.2

EXPOSE 9999
WORKDIR /usr/src/app

ADD streamlit-app-final.py .
ADD .env .
ADD .streamlit ./.streamlit

RUN pip install --upgrade pip
RUN pip install pandas psycopg2-binary python-dotenv streamlit altair

ENV PYTHONUNBUFFERED=1
CMD ["streamlit", "run", "./streamlit-app-final.py", "--server.port", "80"]