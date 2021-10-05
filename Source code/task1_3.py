from psycopg2.extras import execute_values
from psycopg2.errors import UniqueViolation
from multiprocessing import Process
from connection.connect import connect


DEEPSTATE = 'Deep State'
QANON = 'Qanon'
WORLDORDER = 'New world order'
VIRUSESCAPED = 'The virus escaped from a Chinese lab'
GLOBALWARMING = 'GLobal Warming is HOAX'
MICROCHIPPING = 'COVID19 and microchipping'
_5G = 'COVID19 is preaded by 5G'
MOONLANDING = 'Moon landing is fake'
_911 = '9/11 was inside job'
PIZZAGATE = 'Pizzagate conspiracy theory'
CHEMTRAILS = 'Chemtrails'
FLATEARTH = 'Flat Earth'
ILLUMINATI = 'Illuminati'
REPTILIANS = 'Reptilian conspiracy theory'


conspiracy_theories_dict = {
    'deepstatevirus': DEEPSTATE,
    'deepstatevaccine': DEEPSTATE,
    'deepstatefauci': DEEPSTATE,
    'ccpvirus': VIRUSESCAPED,
    'climatechangehoax': GLOBALWARMING,
    'agenda21': WORLDORDER,
    'qanon': QANON,
    'globalwarminghoax': GLOBALWARMING,
    'sorosvirus': MICROCHIPPING,
    'chinaliedpeopledied': VIRUSESCAPED,
    '5gcoronavirus': _5G,
    'maga': QANON,
    'wwg1wga': QANON,
    'chemtrails': CHEMTRAILS,
    'flatearth': FLATEARTH,
    'moonlandinghoax': MOONLANDING,
    'moonhoax': MOONLANDING,
    'illuminati': ILLUMINATI,
    'pizzagateisreal': PIZZAGATE,
    'pedogateisreal': PIZZAGATE,
    '911truth': _911,
    '911insidejob': _911,
    'reptilians': REPTILIANS,
}


def create_conspiracy_table(connection):

    SQL1 = "CREATE TABLE IF NOT EXISTS conspiracy_theories (" \
          " id serial not null constraint conspiracy_theories_pkey primary key," \
          " theory_name text not null," \
          " constraint conspiracy_theories_theory_name_uindex unique (theory_name));"

    SQL2 = "CREATE TABLE IF NOT EXISTS tweet_conspiracy_theories(" \
           " id serial not null constraint tweet_conspiracy_theories_pkey primary key," \
           " theory_id integer not null constraint tweet_conspiracy_theories_theory_id_fkey references conspiracy_theories," \
           " tweet_id varchar(20) not null constraint tweet_conspiracy_theories_tweet_id_fkey references tweets," \
           " constraint tweet_conspiracy_theories_theory_id_tweet_id_key unique (theory_id, tweet_id));"

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(SQL1)
            cursor.execute(SQL2)


def populate_conspiracy_theories_table(connection):
    SQL = "INSERT INTO conspiracy_theories (theory_name)" \
          "VALUES %s;"
    data = [(DEEPSTATE,), (VIRUSESCAPED,), (WORLDORDER,), (QANON,), [GLOBALWARMING],
            (MICROCHIPPING,), (_5G,), (MOONLANDING,), (_911,), (PIZZAGATE,), (CHEMTRAILS,),
            (FLATEARTH,), (ILLUMINATI,), (REPTILIANS,)]

    with connection:
        with connection.cursor() as cursor:
            execute_values(cursor, SQL, data)


def populate_tweet_conspiracy_theories_table(connection):
    SQL_SELECT = "SELECT * from conspiracy_theories;"
    theory_to_id = {}

    with connection.cursor() as cursor:
        cursor.execute(SQL_SELECT)
        for row in cursor.fetchall():
            theory_to_id[row[1]] = row[0]

    processes = [Process(target=_populate_with_one_theory, args=(theory_to_id, t))
                 for t in conspiracy_theories_dict.keys()]

    for p in processes:
        p.start()

    for p in processes:
        p.join()


def _populate_with_one_theory(theory_to_id, conspiracy_hashtag):
    SQL_SELECT = "SELECT DISTINCT tweet_id from tweets " \
                 "JOIN tweet_hashtags th on tweets.id = th.tweet_id " \
                 "JOIN hashtags h on h.id = th.hashtag_id " \
                 "where h.value ilike %s ;"

    SQL_INSERT = "INSERT INTO tweet_conspiracy_theories (theory_id, tweet_id) VALUES (%s, %s);"

    connection = connect('connection/database_config.ini')

    with connection.cursor() as cursor:
        cursor.execute(SQL_SELECT, ('%' + conspiracy_hashtag + '%',))

        for row in cursor.fetchall():
            with connection:
                theory_name = conspiracy_theories_dict[conspiracy_hashtag]
                theory_id = theory_to_id[theory_name]
                tweet_id = row[0]
                try:
                    cursor.execute(SQL_INSERT, (theory_id, tweet_id))
                except UniqueViolation:
                    connection.rollback()