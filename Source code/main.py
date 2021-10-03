# import nltk
# nltk.download('vader_lexicon')

from nltk.sentiment.vader import SentimentIntensityAnalyzer
from connection.connect import connect
import task1_3


def sentiment_test():
    sid = SentimentIntensityAnalyzer()
    text = 'This was a good movie.'
    print_polarity(text, sid)


def print_polarity(text, analyzer):
    result = analyzer.polarity_scores(text)
    print(result)


def execute_task1_3(connection):
    task1_3.create_conspiracy_table(connection)
    task1_3.populate_conspiracy_theories_table(connection)
    task1_3.populate_tweet_conspiracy_theories_table(connection)


if __name__ == '__main__':
    connection = connect('connection/database_config.ini')
    # execute_task1_3(connection)
    connection.close()

