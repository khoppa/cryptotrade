from web_scraper import *
from sentiment_analysis import analyse_sentiments
from redditmetrics_scraper import ScrapeRedditMetrics

from matplotlib import pyplot as plt
def scrape_social_media_handles():
    scrape_socials()
    scrape_cmc_subreddits()
    combine_socials()
    get_nosocial_list()

def scrape_bitcointalk():
    MAX_PAGES = 1000
    forum_urls = ["https://bitcointalk.org/index.php?board=5.0",
            "https://bitcointalk.org/index.php?board=7.0",
            "https://bitcointalk.org/index.php?board=8.0"]
    alt_forum_urls = ["https://bitcointalk.org/index.php?board=67.0",
            "https://bitcointalk.org/index.php?board=159.0",
            "https://bitcointalk.org/index.php?board=240.0",
            "https://bitcointalk.org/index.php?board=161.0",
            "https://bitcointalk.org/index.php?board=197.0",
            "https://bitcointalk.org/index.php?board=198.0",
            "https://bitcointalk.org/index.php?board=238.0",
            "https://bitcointalk.org/index.php?board=224.0"
            ]

    allowed_domains = ["bitcointalk.org",]
    scrape_forums(forum_urls, allowed_domains, max_pages=MAX_PAGES,
            coin='BTC')

    #scrape_forums(alt_forum_urls, allowed_domains, max_pages=MAX_PAGES,
    #        coin='ALTS')

   

def scrape_test():
    MAX_PAGES = 3
    forum_urls = ["https://bitcointalk.org/index.php?board=5.0"]
    allowed_domains = ["bitcointalk.org",]
    scrape_forums(forum_urls, allowed_domains, max_pages=MAX_PAGES,
            coin='test')

def test():
    file_path = 'data/bitcointalk/{}_posts.csv'.format('BTC')
    df = pd.read_csv(file_path, encoding='utf-8')
    timestamps = df['timestamp']
    plt.hist(timestamps, bins=100)
    plt.show()

    #df = df.drop(df.index[-1])
    #print df.tail()
    #df.to_csv(file_path, index=False, encoding='utf-8')

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

    #dates, texts = scrape_subreddits(subreddits, submission_limit=20)

    scrape_bitcointalk()
    """

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
    #scrape_social_media_handles()
    #scrape_subreddit_subs()
