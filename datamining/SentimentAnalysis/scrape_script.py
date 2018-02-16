from web_scraper import *
from sentiment_analysis import analyse_sentiments
from redditmetrics_scraper import ScrapeRedditMetrics
from tqdm import tqdm

from matplotlib import pyplot as plt

coinlist = pd.read_csv('data/coinlist.csv', encoding='utf-8')

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
    #scrape_forums(forum_urls, allowed_domains, max_pages=MAX_PAGES,
    #        coin='BTC')

    scrape_forums(alt_forum_urls, allowed_domains, max_pages=MAX_PAGES,
            coin='ALTS')

   
def scrape_reddit():
    SUBMISSION_LIMIT = 10
    general_subreddits = ["cryptocurrency", "cryptomarkets", "altcoin"]
    scrape_subreddits('general', general_subreddits, SUBMISSION_LIMIT)
    for i, row in tqdm(coinlist.iterrows(), total=coinlist.shape[0]):
        if row['pricedata'] and pd.notnull(row['reddit']):
            subreddits = row['reddit']
            try:
                subreddits = ast.literal_eval(subreddits)
            except (ValueError, SyntaxError):
                pass

            scrape_subreddits(row['Symbol'], subreddits, SUBMISSION_LIMIT) 

def test():
    coinwords = ['Sonic Screw Driver Coin', 'SSD'] 
    scrape_tweets('SSD', coinwords, limit=100)



def scrape_twitter():
    LIMIT = 100
    for i, row in tqdm(coinlist.iterrows(), total=coinlist.shape[0]):
        coinwords = []
        if row['pricedata']: 
            coinwords.append(row['Symbol'])
            coinwords.append(row['CoinName'])
            scrape_tweets(row['Symbol'], coinwords, LIMIT)


def scrape_gt():
    for i, row in tqdm(coinlist.iterrows(), total=coinlist.shape[0]):
        coinwords = []
        if row['pricedata']: 
            coinwords.append(row['Symbol'])
            coinwords.append(row['CoinName'])
            scrape_google_trends(row['Symbol'], coinwords)


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
    #test()
    #scrape_reddit()
    #scrape_bitcointalk()
    #scrape_twitter()
    #scrape_gt()
    #scrape_social_media_handles()
    scrape_subreddit_subs()
