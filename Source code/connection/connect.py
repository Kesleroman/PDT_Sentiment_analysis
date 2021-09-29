import psycopg2
from . import config


def connect(config_filename):
    conn = None
    try:
        params = config.config(config_filename, 'database')
        conn = psycopg2.connect(**params)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    return conn
