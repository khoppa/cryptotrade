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
reload(sys)
sys.setdefaultencoding('utf8')

from datetime import datetime, date, time
from time import mktime, sleep
from tqdm import tqdm

import ast
from calmjs.parse import es5
from collections import Counter

from bs4 import BeautifulSoup
from scrapy.crawler import CrawlerProcess
from scrapy.spiders import CrawlSpider, Rule
from scrapy.exceptions import CloseSpider
from scrapy.linkextractors import LinkExtractor

from twitterscraper import query_tweets
from redditmetrics_scraper import ScrapeRedditMetrics
from pytrends.request import TrendReq

from API_settings import client_id, client_secret, user_agent

from matplotlib import pyplot as plt

date_word_list = ['January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November',
        'December', 'Today'
        ]

class ForumSpider(CrawlSpider):
    name = "forums"
    auto_throttle_enabled = True
    download_delay = 1.5
    rules = (
        Rule(LinkExtractor(
            allow_domains='bitcointalk.org',
            allow=['topic'],
            deny=['profile', 'new', 'msg', 'action', 'prev_next', 'all',
                'print', 'torrent'],
            restrict_xpaths=("//span")),
            callback="parse_posts",
            process_request='filter_moved'
                ),
        Rule(LinkExtractor(
            allow_domains='bitcointalk.org',
            deny=['profile', 'new', 'msg', 'action', 'prev_next', 'all',
                'print','sort'],
            restrict_xpaths=("//a[contains(@class, 'navPages')]")),
            callback="parse_boards",
            process_links='sort_links',
            follow=True,
                ),
             )


    def __init__(self, *args, **kwargs):
        self.coin = kwargs.pop('coin', None)
        super(ForumSpider, self).__init__(*args, **kwargs)
        self.visited = []


    def start_requests(self):
        self.pages_crawled = 0
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse)
            

    def parse_first(self, response):
        """ Parsing method for first urls that are passed into Scrapy.
            These urls are the main board urls for bitcointalk, and thus
            filtering the urls in these responses will cut down on
            processing duplicates and unwanted links.
        """
        return self.parse(response)


    def filter_moved(self, request):
        anchor = request.meta.get('link_text')
        if 'MOVED' in anchor:
            return None
        else:
            return request

    def sort_links(self, links):
        sorted_links = sorted(links,
                key=lambda k: int(k.url.split('=')[-1].split('.')[-1]))
        return sorted_links

    def sort_post_links(self, links):
        return sorted(links, key=lambda k:
                k.url.split('=')[-1].split('.')[-1])


    """
    def filter_doubles(self, links):
        new_links = []
        for link in links:
            if link.url not in self.visited:
                new_links.append(link)

        return new_links
    """


    def parse_page(self, response):
        for link in LinkExtractor(allow_domains=
                self.allow_domains).extract_links(response):
            yield scrapy.Request(url=link.url,
                    callback=self.read_posts_bitcointalk)


    def parse_boards(self, response):
        print 'parse_boards % s' % response.url
        return self.parse(response)


    def parse_posts(self, response):
        self.pages_crawled += 1
        self.check_max_pages()
        #self.visited.append(response.url)
        print 'parse_posts % s' % response.url

        soup = BeautifulSoup(response.body, "html.parser")
        dates, texts, posters = self.get_post_data(soup)
        posts = []
        post = response.url.split('=')[-1]
        [posts.append(post) for d in dates]

        dates, texts, posts, posters = self.parse_posts_additional(response,
                dates, texts, posts, posters)

        file_path = 'data/bitcointalk/{}_posts.csv'.format(self.coin)
        df = pd.read_csv(file_path, encoding='utf-8')
        try:
            new_df = pd.DataFrame({'timestamp':dates, 'text':texts,
                'post':posts, 'author':posters})
        except:
            print 'parsing lengths do not match.'

        new_df['post'] = pd.to_numeric(new_df['post'])
        new_df['text'] = new_df['text'].map(lambda x: x.encode(
            'unicode-escape').decode('utf-8'))
        #df = df.append(new_df, ignore_index=True)
        df = df.merge(new_df, on=list(df), how='outer') 
        df.drop_duplicates(inplace=True, keep='last')
        df.to_csv(file_path, index=False, encoding='utf-8')


    def parse_posts_additional(self, response, dates=[], texts=[], posts=[],
            posters=[]):
        links = LinkExtractor(allow_domains=
                self.allow_domains,
                unique=True,
                restrict_xpaths=("//a[contains(@class, 'navPages')]")
                ).extract_links(response)

        sorted_links = self.sort_links(links)
        for link in sorted_links:
            print 'parse_additional % s' % link.url
            req = requests.get(url=link.url)
            soup  = BeautifulSoup(req.content, "html.parser")
            dates_new, texts_new, posters_new = self.get_post_data(soup)
            [dates.append(d) for d in dates_new]
            [texts.append(t) for t in texts_new]
            [posters.append(p) for p in posters_new]
            [posts.append(link.url.split('=')[-1]) for d in dates_new]
            sleep(self.download_delay)
        return dates, texts, posts, posters


    def get_post_data(self, soup):
        """ Gets dates and text from SOUP, a BeautifulSoup object created
            from a bitcointalk post.
            Args:
                soup(BeautifulSoup): BeautifulSoup object
            Returns:
                dates: list of dates from post
                texts: list of text from post
        """
        tds_raw = soup.find_all('td', class_=['windowbg', 'windowbg2'])
        texts = []
        posters = []
        for td in tds_raw:
            text_raw = td.find('div', class_="post")
            if text_raw:
                quotes = text_raw.find_all('div', class_="quote")
                for q in quotes:
                    q.decompose()
                q_header = text_raw.find_all('div', class_="quoteheader")
                for qh in q_header:
                    qh.decompose()
                text = text_raw.get_text().encode('utf-8')
                if not text.isdigit():
                    texts.append(text)
                    poster = td.find('td', class_='poster_info').b.a['href']
                    posters.append(poster.split('=')[-1])


        dates_raw = soup.find_all('div', class_="smalltext")

        dates = []
        for date in dates_raw:
            date = date.get_text()
            if any(substring in date for substring in date_word_list) \
                and len(date) < 32:
                date = convert_date_to_unix_time(date)
                dates.append(date)

        return dates, texts, posters


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


def scrape_forums(url, allowed_domain, max_pages, coin):
    sys.setrecursionlimit(10000)

    file_path = 'data/bitcointalk/{}_posts.csv'.format(coin)
    if not os.path.exists(file_path):
        df = pd.DataFrame(columns=['timestamp', 'text', 'post', 'author'])
        df.to_csv(file_path, index=False, encoding='utf-8')


    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
    })

    spider = ForumSpider()

    process.crawl(spider, start_urls=url,
            allow_domains=allowed_domain, max_pages=max_pages, coin=coin)
    process.start()
    process.stop()


def scrape_subreddit(subreddit, submission_limit):
    dates = []
    texts = []
    posts = []
    authors = []

    reddit = praw.Reddit(client_id=client_id,
                         client_secret=client_secret,
                         user_agent=user_agent)

    subreddit = reddit.subreddit(subreddit)
    try:
        for submission in subreddit.hot(limit=submission_limit):
            dates.append(submission.created)
            texts.append(submission.selftext)
            posts.append(submission.id)
            if submission.author:
                authors.append(submission.author.name)
            else:
                authors.append('None')

            for comment in submission.comments[:]:
                if hasattr(comment, 'created'):
                    dates.append(comment.created)
                    texts.append(comment.body)
                    posts.append(submission.id)
                    if comment.author:
                        authors.append(comment.author.name)
                    else:
                        authors.append('None')
                else:
                    pass
    except:
        print subreddit

    return dates, texts, posts, authors


def scrape_subreddits(coin, subreddits, submission_limit):
    dates = []
    texts = []
    posts = []
    authors = []

    if isinstance(subreddits, basestring):
        subreddits = [subreddits]
    for subreddit in subreddits:
        dates_temp, texts_temp, posts_temp, authors_temp = scrape_subreddit(
                subreddit, submission_limit)

        dates += dates_temp
        texts += texts_temp
        posts += posts_temp
        authors += authors_temp

    df = pd.DataFrame({'timestamp':dates, 'text':texts,
            'post':posts, 'author':authors})
    df['text'] = df['text'].map(
            lambda x: x.encode('unicode-escape').decode('utf-8'))
    df['author'] = df['author'].map(
            lambda x: x.encode('unicode-escape').decode('utf-8'))


    file_path = 'data/subreddits/{}.csv'.format(coin)
    add_data(file_path, df)

def scrape_subreddit_subs():
    """ Scrapes historical subscriber count for SUBREDDIT,
        using redditmetrics.com. Writes data to COIN.csv
    """
    base_url = 'http://redditmetrics.com/r/'
    coinlist = pd.read_csv('data/coinlist.csv')
    for i, row in tqdm(coinlist.iterrows(), total=coinlist.shape[0]):
        if row['pricedata'] == True and pd.notnull(row['reddit']):
            coin = row['Symbol']
            subreddits = row['reddit'].decode('unicode_escape').encode(
                    'ascii', 'ignore')
            try:
                subreddits = ast.literal_eval(subreddits)
                df = pd.DataFrame(columns=['date', 'subscriber_count'])
                for s in subreddits:
                    url = base_url + s
                    metrics = ScrapeRedditMetrics(url)
                    df_temp = metrics.scrape_and_parse()
                    if df_temp is not None:
                        df = df.merge(df_temp, on='date',
                                how='outer', suffixes=['', '_'+s])
                df.drop('subscriber_count', axis=1, inplace=True)
                file_path = 'data/redditmetrics/{}.csv'.format(coin)
                add_data(file_path, df)

            except (ValueError, SyntaxError):
                url = base_url + subreddits
                metrics = ScrapeRedditMetrics(url)
                df = metrics.scrape_and_parse()
                if df is None:
                    print '{} doesnt have subreddit subscriber data'.format(
                            subreddits)
                else:
                    df.to_csv('data/redditmetrics/{}.csv'.format(coin),
                            index=False)


def scrape_tweets(coin, coin_words, limit=100):
    """ Scrapes Twitter for tweets with COIN_WORDS.
        Args:
            coin_words(list): list of cryptocurrency key words.

    """
    query = coin_words[0]
    for word in coin_words[1:]:
        add = ' OR ' + word
        query += add

    list_of_tweets = query_tweets(query, limit=limit, lang='en')

    list_of_tweets = [vars(x) for x in list_of_tweets]
    df = pd.DataFrame(list_of_tweets)
    try:
        df.drop(columns=['fullname', 'url'], inplace=True)
        df['timestamp'] = df['timestamp'].astype(int) // (10 ** 9)
        df['text'] = df['text'].map(
                lambda x: x.encode('unicode-escape').decode('utf-8'))

        file_path = 'data/twitter/{}.csv'.format(coin)
        add_data(file_path, df)
    except Exception as e:
        print '{} produced {}'.format(coin, e)

def scrape_google_trends(coin, coin_words):
    """ Scrapes Google Trends for trend data of interest over time
        based on COIN_WORDS.
    """
    pytrends = TrendReq(hl='en-US', tz=0)
    pytrends.build_payload(kw_list=coin_words)
    try: 
        df = pytrends.interest_over_time()
        df['timestamp'] = df.index.values.astype(int) // (10 ** 9)
        file_path = 'data/googletrends/{}.csv'.format(coin)
        add_data(file_path, df)

    except:
        print '{} produced error.'.format(coin)


def add_data(file_path, df):
    """ Method for adding scraped data. Checks if file already exists
        for scraped data. If it exists, appends to existing file.
        Args:
            file_path (str): Path for saving DataFrame
            df (pd.DataFrame): DataFrame with new scraped data.
    """
    if os.path.exists(file_path):
        old_df = pd.read_csv(file_path, encoding='utf-8')
        df = old_df.merge(df, on=list(old_df), how='outer') 
        df.drop_duplicates(inplace=True, keep='last')
 
    df.to_csv(file_path, index=False, encoding='utf-8')


"""
Methods for scraping social media handles. Used in succession to 
grab social media handles for coins from different sources, then
aggregated and cleaned to be manually processed at the end.
"""

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

    df.to_csv('data/output/cleaned_coinlist.csv', index=False, encoding='utf-8')


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

    df.to_csv('data/output/cleaned_coinlist.csv', index=False, encoding='utf-8')

 
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
