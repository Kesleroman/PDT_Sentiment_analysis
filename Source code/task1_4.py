
def create_table(connection):
    SQL = """
            create table if not exists conspiracy_tweets_weekly
            (
                week                timestamp with time zone,
                tweet_extreme_count integer,
                tweet_neutral_count integer,
                tweet_count         integer
            );"""

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(SQL)


def populate_conspiracy_tweets_weekly_table(connection):
    SQL = """   INSERT INTO conspiracy_tweets_weekly  
                SELECT  coalesce(extreme_week, neutral_week)     as week,
                        coalesce(extreme.tweet_extreme_count, 0) as tweet_extreme_count,
                        coalesce(tweet_neutral_count , 0)        as tweet_neutral_count,
                        coalesce(extreme.tweet_extreme_count, 0) + coalesce(tweet_neutral_count , 0)
                FROM            
                    (SELECT count(*) tweet_extreme_count, date_trunc('week', t.happened_at) extreme_week 
                    FROM tweets t
                    JOIN tweet_conspiracy_theories tct on t.id = tct.tweet_id
                    WHERE (t.compound > 0.5 or t.compound < -0.5)
                    GROUP BY extreme_week ) extreme
                FULL OUTER JOIN 
                    (SELECT count(*) tweet_neutral_count, date_trunc('week', t.happened_at) neutral_week  
                    FROM tweets t
                    JOIN tweet_conspiracy_theories tct on t.id = tct.tweet_id
                    WHERE (t.compound <= 0.5 and t.compound >= -0.5)
                    GROUP BY neutral_week ) neutral ON extreme_week = neutral_week
                ORDER BY week;"""

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(SQL)

