from web_scraper import *
from sentiment_analysis import analyse_sentiments
from redditmetrics_scraper import ScrapeRedditMetrics

from matplotlib import pyplot as plt
def scrape_social_media_data():
    scrape_socials()
    scrape_cmc_subreddits()
    combine_socials()
    get_nosocial_list()

def import_sentiments():
    """ Imports sentiment analysis data. Grabs social media links
        from SentimentAnalysis/data/coinlist.csv, and performs
        sentiment analysis via bitcointalk, twitter, reddit, and
        google trends.
    """
    return None

if __name__ == '__main__':
    bitcoin_keywords = ['bitcoin', 'bitcoins', 'xbt', 'btc', 'Bitcoin',
            'Bitcoins', 'BTC', 'XBT']
    subreddits = ["cryptocurrency", "cryptomarkets", "bitcoin",
            "bitcoinmarkets"]

    forum_urls = ["https://bitcointalk.org/index.php?board=5.0",
            "https://bitcointalk.org/index.php?board=7.0",
            "https://bitcointalk.org/index.php?board=8.0"]
    allowed_domains = ["bitcointalk.org",]

    """
    dates, texts = scrape_subreddits(subreddits, submission_limit=20)

    #dates = []
    #texts = []
    #dates_temp, texts_temp = scrape_forums(forum_urls, allowed_domains,
    #        max_pages=20)

    #dates += dates_temp
    #texts += texts_temp

    dates_temp, texts_temp = scrape_twitter(bitcoin_keywords)

    dates += dates_temp.tolist()
    texts += texts_temp.tolist()

    dates, texts, sentiments = analyse_sentiments(dates, texts, bitcoin_keywords)

    plt.figure(1)
    plt.plot(dates, sentiments, "o")
    plt.show()
    """
    btc_words = ["bitcoin", "bitcoins", "btc", "xbt"]
    #scrape_google_trends(btc_words)
    #scrape_social_media_data()


    scrape_subreddit_subs()
