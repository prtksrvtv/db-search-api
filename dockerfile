FROM python:3.8-slim-buster

WORKDIR /db-plug

RUN apt-get update \
    && apt-get -y install libpq-dev gcc \
    && pip install psycopg2

COPY requirements.txt /
RUN pip install --no-cache-dir --upgrade -r /requirements.txt

COPY . .

EXPOSE 5000

#CMD [ "flask", "run","--host","0.0.0.0","--port","5000"]

CMD ["gunicorn", "db_search:app", "-b", "0.0.0.0:5000"]