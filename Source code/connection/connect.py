import psycopg2
from . import config


def connect(filename):
    conn = None
    try:
        params = config.config(filename, 'database')
        conn = psycopg2.connect(**params)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    return conn
