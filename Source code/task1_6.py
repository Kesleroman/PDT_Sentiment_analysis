from psycopg2.extras import execute_batch
from multiprocessing import Process
from connection.connect import connect


def create_table(connection):
    SQL = """
        create table if not exists hashtags_because_of_which_you_go_to_jail(
            theory_id     integer,
            hashtag_count integer,
            hashtag_value varchar(200)
        );"""

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(SQL)


def populate_table(connection):

    with connection.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM conspiracy_theories;")
        conspiracy_theories_num = cursor.fetchone()[0]

        half = conspiracy_theories_num // 2
        offset = 1 if conspiracy_theories_num % 2 == 0 else 2

        p1 = Process(target=_populate_table, args=(range(1, half + offset),))
        p2 = Process(target=_populate_table, args=(range(half + offset, conspiracy_theories_num + 1),))

        p1.start()
        p2.start()
        p1.join()
        p2.join()


def _populate_table(id_range):
    connection = connect('connection/database_config.ini')

    with connection:
        with connection.cursor() as cursor:
            SQL = """   INSERT INTO hashtags_because_of_which_you_go_to_jail
                            SELECT theory_id, count(*) hashtag_count, h.value FROM tweet_conspiracy_theories
                            JOIN tweets t on t.id = tweet_conspiracy_theories.tweet_id
                            JOiN tweet_hashtags th on t.id = th.tweet_id
                            JOIN hashtags h on h.id = th.hashtag_id
                            WHERE theory_id = $1 and (t.compound > 0.5 or t.compound < -0.5)
                            GROUP BY hashtag_id, h.value, theory_id
                            ORDER BY hashtag_count DESC
                            LIMIT 10;"""

            args = [(i,) for i in id_range]
            cursor.execute("PREPARE stmt AS " + SQL)
            execute_batch(cursor, "EXECUTE stmt (%s)", args)
            cursor.execute("DEALLOCATE stmt")
