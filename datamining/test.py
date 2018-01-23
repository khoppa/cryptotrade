from common import *
from scrape import Scraper
from dataminer import Miner
from SentimentAnalysis.web_scraper import ForumSpider

import praw
import scrapy
import pickle
import os
import sys
from datetime import datetime, date, time
from time import mktime
from bs4 import BeautifulSoup
from scrapy.crawler import CrawlerProcess
from scrapy.spiders import Rule
from scrapy.exceptions import CloseSpider
from scrapy.linkextractors import LinkExtractor


"""
url = 'https://min-api.cryptocompare.com/data/all/exchanges'
scraper = Scraper()
df = scraper.get_tradepair_data(url)

miner = Miner()
major_exchanges = ["BitTrex", "Bitfinex", "Bithumb", "Bitstamp", \
        "Coinbase", "Gemini", "bitFlyer", "Binance", "Coinone", \
        "HitBTC", "Poloniex", "Kraken", "Korbit", "Huobi"]

major_exchanges = ['Binance']
#coins = miner.get_exchange_coins(major_exchanges)
#miner.make_dataset('Binance', 'BTM')
#miner.import_supply()
coinlist = []
for f in listdir('data/exchanges'):
    if f.endswith('.csv'):
        coinlist.append(f.split(".")[0])
idx = coinlist.index('UET')
print coinlist[idx:]
date_word_list = ['January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November',
        'December', 'Today'
        ]
class TestSpider(ForumSpider):
    def read_posts_bitcointalk(self, response):
        url_post_string = ['topic', ]

        if any(substring in response.url for substring in url_post_string):
            self.pages_crawled += 1
            self.check_max_pages()

            soup = BeautifulSoup(response.body, "html.parser")
            texts_raw = soup.find_all('div', class_="post")
            dates_raw = soup.find_all('div', class_="smalltext")

            dates = []
            for date in dates_raw:
                date = date.get_text()
                print date
                if any(substring in date for substring in date_word_list) \
                    and len(date) < 32:
                    date = convert_date_to_unix_time(date)
                    dates.append(date)
                    print date
            print len(dates)

            texts = []
            for text in texts_raw:
                text = text.get_text().encode('utf-8')
                if not text.isdigit():
                    texts.append(text)

            filename_date = "temp_date_output.txt"
            filename_text = "temp_text_output.txt"

            try:
                os.remove(filename_date)
            except OSError:
                pass

            try:
                os.remove(filename_text)
            except OSError:
                pass

            with open(filename_date, "a") as f1:
                pickle.dump(dates, f1)

            with open(filename_text, "a") as f2:
                pickle.dump(texts, f2)

        url_board_string = ["board=5", "board=7", "board=8"]
        if any(substring in response.url for substring in url_board_string):
            self.parse(response)

def convert_date_to_unix_time(date_local):
    if 'Today at ' in date_local:
        date_local = date_local.replace('Today at ', '')
        midnight = float(mktime(datetime.combine(date.today(),
            time.min).timetuple()))
        date_local = midnight + mktime(datetime.strptime(date_local,
            "%I:%M:%S %p").timetuple())
    else:
        date_local = mktime(datetime.strptime(date_local,
            "%B %d, %Y, %I:%M:%S %p").timetuple())

    return date_local


def scrape_forums(url, allowed_domain, max_pages):
    sys.setrecursionlimit(10000)
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
    })

    spider = TestSpider()

    process.crawl(spider, start_urls=url,
            allow_domains=allowed_domain, max_pages=max_pages)
    process.start()
    process.stop()

bitcoin_keywords = ['bitcoin', 'bitcoins', 'xbt', 'btc', 'Bitcoin',
        'Bitcoins', 'BTC', 'XBT']
subreddits = ["cryptocurrency", "cryptomarkets", "bitcoin",
        "bitcoinmarkets"]

forum_urls = ["https://bitcointalk.org/index.php?board=8.0"]
allowed_domains = ["bitcointalk.org",]

#dates_temp, texts_temp = scrape_forums(forum_urls, allowed_domains,
#        max_pages=1)

"""
url = 'https://www.cryptocompare.com/api/data/coinlist'
scraper = Scraper()
df = scraper.get_api_data(url)
df = df.transpose()

if df is not None:
    coinlist = df[['Symbol', 'CoinName', 'Id']]
    coinlist['Id'] = coinlist['Id'].astype(np.int64)
    coinlist.loc[:, 'pricedata'] = False
    coinlist.reset_index(drop=True, inplace=True)

old_cl = pd.read_csv('data/coinlist.csv', encoding='utf-8')
old_cl = old_cl.ix[:500]

diff = pd.concat([old_cl, coinlist])
diff = diff.reset_index(drop=True)
diff_gp = diff.groupby(list(diff.columns)[0])
idx = [x[0] for x in diff_gp.groups.values() if len(x) == 1]
diff_coins = diff.reindex(idx)['Symbol']

print len(idx)
print len(coinlist)
print len(old_cl)
print len(diff.iloc[idx])
old_cl = old_cl.append(diff.iloc[idx], ignore_index=True)
print len(old_cl)
