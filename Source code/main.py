from connection.connect import connect
import task1_6
import task1_5
import task1_4
import task1_3
import task1_2
from nltk.sentiment.vader import SentimentIntensityAnalyzer


def sentiment_test():
    sid = SentimentIntensityAnalyzer()
    text = 'This was a good movie.'
    print_polarity(text, sid)


def print_polarity(text, analyzer):
    result = analyzer.polarity_scores(text)
    print(result)


def execute_task1_3():
    task1_3.create_conspiracy_table(connection)
    task1_3.populate_conspiracy_theories_table(connection)
    task1_3.populate_tweet_conspiracy_theories_table(connection)


def execute_task1_2():
    task1_2.populate_tweets_table_with_sentiments(connection)


def execute_task1_4():
    task1_4.create_table(connection)
    task1_4.populate_conspiracy_tweets_weekly_table(connection)


def execute_task1_5():
    task1_5.create_table(connection)
    task1_5.populate_active_conspirators_table(connection)


def execute_task1_6():
    task1_6.create_table(connection)
    task1_6.populate_table(connection)


if __name__ == '__main__':
    connection = connect('connection/database_config.ini')
    # execute_task1_2()
    # execute_task1_3()
    # execute_task1_4()
    # execute_task1_5()
    # execute_task1_6()
    # sentiment_test()
    connection.close()

