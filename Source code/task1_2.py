# import nltk
# nltk.download('vader_lexicon')
import re
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from multiprocessing import Process
from connection.connect import connect

NUM_PROCESSES = 4
hashtag_pattern = re.compile(r'#\w+\b')
mention_pattern = re.compile(r'@\w+\b')
emoji_pattern = re.compile(r'&\w+;')


def populate_tweets_table_with_sentiments(connection):
    SQL_SELECT = "SELECT DISTINCT count(*) from tweets " \
                 "JOIN tweet_conspiracy_theories tct " \
                 "ON tweets.id = tct.tweet_id;"

    with connection.cursor() as cursor:
        cursor.execute(SQL_SELECT)
        num_rows = int(cursor.fetchone()[0])

    processes = [Process(target=_calculate_sentiment_and_insert,
                         args=(num_rows // NUM_PROCESSES + 1, i * (num_rows // NUM_PROCESSES + 1)))
                 for i in range(NUM_PROCESSES)]

    for p in processes:
        p.start()

    for p in processes:
        p.join()


def _calculate_sentiment_and_insert(limit, offset):
    SQL_SELECT = "SELECT DISTINCT tweets.id from tweets " \
                 "JOIN tweet_conspiracy_theories tct " \
                 "ON tweets.id = tct.tweet_id " \
                 "LIMIT %s " \
                 "OFFSET %s;"

    SQL_SELECT_CONTENT = "SELECT content from tweets " \
                         "where id = %s;"

    SQL_UPDATE = "UPDATE tweets SET neg = %s, neu = %s, pos = %s, compound = %s WHERE id = %s;"

    print(limit, offset)

    analyzer = SentimentIntensityAnalyzer()
    connection = connect('connection/database_config.ini')

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(SQL_SELECT, (limit, offset))

            for row in cursor.fetchall():

                with connection.cursor() as cur:
                    cur.execute(SQL_SELECT_CONTENT, (row[0],))
                    content = cur.fetchone()[0]

                result = analyzer.polarity_scores(content)
                cursor.execute(SQL_UPDATE, (result['neg'], result['neu'], result['pos'], result['compound'], row[0]))

