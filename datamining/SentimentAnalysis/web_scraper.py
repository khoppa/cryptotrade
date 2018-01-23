import praw
import re
import json
import pandas as pd
import numpy as np
import requests
import scrapy
import pickle
import os
import sys
from datetime import datetime, date, time
from time import mktime

import ast
from calmjs.parse import es5
from collections import Counter

from bs4 import BeautifulSoup
from scrapy.crawler import CrawlerProcess
from scrapy.spiders import Rule
from scrapy.exceptions import CloseSpider
from scrapy.linkextractors import LinkExtractor

from twitterscraper import query_tweets
from pytrends.request import TrendReq

from API_settings import client_id, client_secret, user_agent

from matplotlib import pyplot as plt

date_word_list = ['January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November',
        'December', 'Today'
        ]


class ForumSpider(scrapy.Spider):
    name = "forums"
    auto_throttle_enabled = True
    download_delay = 1.5
    rules = (Rule(LinkExtractor(), callback="parse", follow=True),
             )

    def start_requests(self):
        self.pages_crawled = 0
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        for link in LinkExtractor(allow_domains=
                self.allow_domains).extract_links(response):
            yield scrapy.Request(url=link.url,
                    callback=self.read_posts_bitcointalk)

    def read_posts_bitcointalk(self, response):
        url_post_string = ['topic', ]

        if any(substring in response.url for substring in url_post_string):
            self.pages_crawled += 1
            self.check_max_pages()

            soup = BeautifulSoup(response.body, "html.parser")
            texts_raw = soup.find_all('div', class_="post")
            for t in texts_raw:
                quotes = t.find_all('div', class_="quote")
                for q in quotes:
                    q.decompose()
                q_header = t.find_all('div', class_="quoteheader")
                for qh in q_header:
                    qh.decompose()
            dates_raw = soup.find_all('div', class_="smalltext")

            dates = []
            for date in dates_raw:
                date = date.get_text()
                if any(substring in date for substring in date_word_list) \
                    and len(date) < 32:
                    date = convert_date_to_unix_time(date)
                    dates.append(date)

            texts = []
            for text in texts_raw:
                text = text.get_text().encode('utf-8')
                if not text.isdigit():
                    texts.append(text)

            filename_date = "temp_date_output.txt"
            filename_text = "temp_text_output.txt"

            with open(filename_date, "a") as f1:
                pickle.dump(dates, f1)

            with open(filename_text, "a") as f2:
                pickle.dump(texts, f2)

        url_board_string = ["board=5", "board=7", "board=8"]
        if any(substring in response.url for substring in url_board_string):
            self.parse(response)

    def check_max_pages(self):
        if self.pages_crawled > self.max_pages:
            raise CloseSpider(reason='Page number exceeded')


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


    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
    })

    spider = ForumSpider()

    process.crawl(spider, start_urls=url,
            allow_domains=allowed_domain, max_pages=max_pages)
    process.start()
    process.stop()

    dates = []
    texts = []
    with open("temp_date_output.txt", "r") as f1:
        while 1:
            try:
                dates_temp = pickle.load(f1)
                for d in dates_temp:
                    dates.append(d)
            except EOFError:
                break

    with open("temp_text_output.txt", "r") as f2:
        while 1:
            try:
                texts_temp = pickle.load(f2)
                for t in texts_temp:
                    texts.append(t)
            except EOFError:
                break

    return dates, texts


def scrape_subreddit(subreddit, submission_limit):
    dates = []
    texts = []

    reddit = praw.Reddit(client_id=client_id,
                         client_secret=client_secret,
                         user_agent=user_agent)

    for submission in reddit.subreddit(subreddit).hot(limit=submission_limit):
        dates.append(submission.created)
        texts.append(submission.selftext)

        for comment in submission.comments[:]:
            if hasattr(comment, 'created'):
                dates.append(comment.created)
                texts.append(comment.body)
            else:
                pass

    return dates, texts


def scrape_subreddits(subreddits, submission_limit):
    dates_local = []
    texts_local = []
    for subreddit in subreddits:
        dates_temp, texts_temp = scrape_subreddit(subreddit, submission_limit)

        dates_local += dates_temp
        texts_local += texts_temp

    return dates_local, texts_local


def scrape_subreddit_subs(subreddit, submission_limit):
    """ Scrapes historical subscriber count for SUBREDDIT,
        using redditmetrics.com.
    """
    return None


def scrape_twitter(coin_words):
    """ Scrapes Twitter for tweets with COIN_WORDS.
        Args:
            coin_words(list): list of cryptocurrency key words.

    """
    query = coin_words[0]
    for word in coin_words[1:]:
        add = ' OR ' + word
        query += add

    list_of_tweets = query_tweets(query, limit=100, lang='en')

    list_of_tweets = [vars(x) for x in list_of_tweets]
    df = pd.DataFrame(list_of_tweets)
    df.drop(columns=['fullname', 'id', 'url'], inplace=True)
    df['timestamp'] = df['timestamp'].astype(int) // (10 ** 9)
    return df['timestamp'], df['text']


def scrape_google_trends(coin_words):
    """ Scrapes Google Trends for trend data of interest over time
        based on COIN_WORDS.
    """
    pytrends = TrendReq(hl='en-US', tz=0)
    pytrends.build_payload(kw_list=coin_words)
    df = pytrends.interest_over_time()
    print df.tail()


def scrape_socials():
    """ Gets social media sites for twitter and reddit from cryptocompare.
    """
    parent_dir = os.path.abspath('..')
    coinlist_path = os.path.join(parent_dir, 'data/coinlist.csv')
    df = pd.read_csv(coinlist_path, encoding='utf-8')
    for i, row in df.iterrows():
        if row['pricedata'] == True:
            coinid = row['Id']
            url = 'https://www.cryptocompare.com/api/data/socialstats/?id=' + \
                    str(coinid)
            social_df = get_social_data(url)
            if social_df.loc['Twitter', 'Points'] != 0:
                twitter = social_df.loc['Twitter', 'link'].split('/')[-1]
                df.loc[i, 'twitter'] = twitter
            if social_df.loc['Reddit', 'Points'] != 0:
                reddit = social_df.loc['Reddit', 'link'].split('/')[-2]
                df.loc[i, 'reddit'] = reddit

    df.to_csv(coinlist_path, index=False, encoding='utf-8')


def clean_socials():
    """ Removes https://twitter.com/ and https://reddit.com/r/ from socials
        scraped from CryptoCompare
    """
    parent_dir = os.path.abspath('..')
    coinlist_path = os.path.join(parent_dir, 'data/coinlist.csv')
    df = pd.read_csv(coinlist_path, encoding='utf-8')

    for i, row in df.iterrows():
        if not pd.isnull(row['twitter']):
            twitter = row['twitter'].split('/')[-1]
            df.loc[i, 'twitter'] = twitter
        if not pd.isnull(row['reddit']):
            reddit = row['reddit'].split('/')[-2]
            df.loc[i, 'reddit'] = reddit

    df.to_csv('data/output/cleaned_coinlist.csv', encoding='utf-8')

 
def search_potential_subreddits():
    """ Gets potential subreddits for coins that have pricedata, from
        coinlist.csv. Does this by searching for the coin symbol and
        coin name separately through reddit.
        Saves file into potential_subs.csv.
    """
    parent_dir = os.path.abspath('..')
    coinlist_path = os.path.join(parent_dir, 'data/coinlist.csv')
    df = pd.read_csv(coinlist_path, encoding='utf-8')

    reddit = praw.Reddit(client_id=client_id,
                         client_secret=client_secret,
                         user_agent=user_agent)

    potential_subs = pd.DataFrame(columns=['Symbol', 'CoinName', 'Subreddits'])

    for i, row in df.iterrows():
        if row['pricedata'] == True:
            coinwords = [row['Symbol'], row['CoinName']]
            subs = []
            for w in coinwords:
                try:
                    #subs_temp = reddit.subreddits.search_by_topic(w)
                    #[subs.append(s.display_name) for s in subs_temp]
                    subs_temp = reddit.subreddits.search(w)
                    [subs.append(s.display_name) for s in subs_temp \
                            if s.display_name not in subs]
                except Exception, e:
                    print 'Error for {}'.format(w)
                    pass
            
            potential_subs.loc[len(potential_subs)] = [row['Symbol'],
                    row['CoinName'], subs]

    #potential_subs.to_json('data/potential_subs.json',
    #        orient='index', force_ascii=False)
    potential_subs.to_csv('data/output/potential_subs.csv', index=False)


def shorten_potential_subreddits():
    df = pd.read_csv('data/output/potential_subs.csv')
    df['Subreddits'] = df['Subreddits'].apply(ast.literal_eval)

    #df = pd.read_json('data/potential_subs.json', orient='index', encoding='utf-8')
    #with open('data/potential_subs.json') as data_file:
    #    df = json.load(data_file, encoding='utf-8')

    subs = []
    """
    for x in df:
        for s in df[x]['Subreddits']:
            subs.append(s)
    """
    for i, row in df.iterrows():
        for s in row['Subreddits']:
            subs.append(s)
    sub_count = dict(((e, subs.count(e)) for e in set(subs)), reverse=True)
    for i, row in df.iterrows():
        coinsubs = row['Subreddits']
        newsubs = []
        for s in coinsubs:
            if sub_count[s] < 3:
                newsubs.append(s)
        df.loc[i, 'Subreddits'] = newsubs 
    df.to_csv('data/output/potential_subs_filtered.csv', index=False)


def scrape_cmc_subreddits():
    """ Gets potential subreddits from CoinMarketCap. Searches the 
        coin and scrapes the resulting site.
    """
    parent_dir = os.path.abspath('..')
    coinlist_path = os.path.join(parent_dir, 'data/coinlist.csv')
    df = pd.read_csv(coinlist_path, encoding='utf-8')

    url = 'https://coinmarketcap.com/currencies/search/?q='

    potential_subs = pd.DataFrame(columns=['Symbol', 'CoinName', 'Subreddits',
        'Twitter'])

    for i, row in df.iterrows():
        if row['pricedata'] == True:
            coin = row['CoinName']
            reddit = ""
            twitter = ""
            try:
                response = requests.get(url + coin).content
                soup = BeautifulSoup(response)
                if len(soup.find_all('p', 'alert alert-warning')) == 0:
                    script = soup.find_all('script')
                    for s in script:
                        pattern = re.compile(r'oScript\.src\s=\s(".*?")',
                                re.MULTILINE | re.DOTALL)
                        match = pattern.search(s.text)
                        if match:
                            link = match.group(1)
                            reddit = link.split('/')[-1].split('.')[0]
                    twitter_link = soup.find('a', 'twitter-timeline')
                    if twitter_link:
                        twitter = twitter_link['href'].split('/')[-1]
                    
             
            except Exception, e:
                print 'Error for {}'.format(w)
                pass
            
            potential_subs.loc[len(potential_subs)] = [row['Symbol'],
                    row['CoinName'], reddit, twitter]

    potential_subs.to_csv('data/output/cmc_subs.csv', index=False)

def combine_socials():
    """ Combines social media sites scraped from CoinMarketCap with 
        coinlist.csv, which already has social media sites scraped 
        from CryptoCompare.
    """
    coinlist_path = 'data/output/cleaned_coinlist.csv'
    df = pd.read_csv(coinlist_path, encoding='utf-8')
    cmc = pd.read_csv('data/output/cmc_subs.csv') 
    #cmc.replace(r'\s+', np.nan, regex=True, inplace=True)

    for i, row in df.iterrows():
        if row['pricedata'] == True:
            coin = row['CoinName']
            cmc_sub = cmc.loc[cmc['CoinName'] == coin, 'Subreddits']
            df_reddit = row['reddit']
            if not cmc_sub.isnull().any(): 
                cmc_sub = str(cmc_sub.values[0])
                if pd.isnull(row['reddit']):
                    df.loc[i, 'reddit'] = cmc_sub
                elif cmc_sub.lower() not in df_reddit.lower():
                    df.loc[i, 'reddit'] = str([df_reddit.encode('ascii',
                        'ignore'), cmc_sub])

            cmc_twitter = cmc.loc[cmc['CoinName'] == coin, 'Twitter']
            df_twitter = row['twitter']
            if not cmc_twitter.isnull().any():
                cmc_twitter = str(cmc_twitter.values[0])
                if pd.isnull(df_twitter):
                    df.loc[i, 'twitter'] = cmc_twitter
                elif cmc_twitter.lower() not in df_twitter.lower():
                    df.loc[i, 'twitter'] = str([df_twitter.encode('ascii',
                        'ignore'), cmc_twitter])

    df.to_csv('data/output/combined_socials.csv', index=False, encoding='utf-8')


def get_nosocial_list():
    """ Returns coins with no social media sites. These will be dealt
        manually to see if the scripts missed data from CryptoCompare or
        CoinMarketCap.
    """
    df = pd.read_csv('data/output/combined_socials.csv')
    nosocials = []
    for i, row in df.iterrows():
        if row['pricedata'] == True:
            if pd.isnull(row['twitter']) or pd.isnull(row['reddit']):
                nosocials.append(row['CoinName'])

    with open('data/output/nosocials.txt', 'w') as f:
        for item in nosocials:
            f.write('{}\n'.format(item))
    
 
def get_social_data(url=''):
    """ Gets api data from URL from CryptoCompare.
    """
    response = requests.get(url).json()
    
    if response['Response'] == 'Success':
        data = response['Data']
        df = pd.DataFrame(data)
        return df.transpose()
    else:
        raise ValueError("API Pull unsuccessful") 
        return None

