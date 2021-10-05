from psycopg2.extras import execute_batch
from multiprocessing import Process
from connection.connect import connect


def create_table(connection):
    SQL = """
        create table if not exists active_conspirators
        (
            id          bigint not null,
            name        varchar(200),
            screen_name varchar(200),
            theory_id   integer,
            tweet_count integer
        );
    """

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(SQL)


def populate_active_conspirators_table(connection):

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
            SQL = """   INSERT INTO active_conspirators
                        SELECT * from
                            ( Select tweets.author_id, a.name, a.screen_name, tct.theory_id, count(*) tweet_count 
                            from tweets
                            JOIN accounts a on a.id = tweets.author_id
                            Join tweet_conspiracy_theories tct on tweets.id = tct.tweet_id
                            WHERE compound > 0.5 or compound < -0.5
                            GROUP BY tweets.author_id, tct.theory_id, a.name, a.screen_name
                            ORDER BY tweet_count DESC ) t
                        WHERE theory_id = $1
                        LIMIT 10;"""

            args = [(i,) for i in id_range]
            cursor.execute("PREPARE stmt AS " + SQL)
            execute_batch(cursor, "EXECUTE stmt (%s)", args)
            cursor.execute("DEALLOCATE stmt")

