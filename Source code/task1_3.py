import psycopg2


def create_conspiracy_table(connection):

    SQL1 = "CREATE TABLE IF NOT EXISTS conspiracy_theories (" \
          " id serial not null constraint conspiracy_theories_pkey primary key," \
          " theory_name text not null" \
          ");"

    SQL2 = "CREATE TABLE IF NOT EXISTS tweet_conspiracy_theories(" \
           " id serial not null constraint tweet_conspiracy_theories_pkey primary key," \
           " theory_id integer not null constraint tweet_conspiracy_theories_theory_id_fkey references hashtags," \
           " tweet_id varchar(20) not null constraint tweet_conspiracy_theories_tweet_id_fkey references tweets," \
           " constraint tweet_conspiracy_theories_theory_id_tweet_id_key unique (theory_id, tweet_id));"

    cursor = connection.cursor()
    cursor.execute(SQL1)
    cursor.execute(SQL2)
    connection.commit()
