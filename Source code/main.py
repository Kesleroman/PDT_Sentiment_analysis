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


if __name__ == '__main__':
    sentiment_test()
    connection = connect('connection/database_config.ini')
    task1_3.create_conspiracy_table(connection)
    connection.close()

