from datetime import datetime, date, time
from time import mktime
import sys
import os

import scrapy
from bs4 import BeautifulSoup
from scrapy.crawler import CrawlerProcess
from scrapy.spiders import CrawlSpider, Rule
from scrapy.exceptions import CloseSpider
from scrapy.linkextractors import LinkExtractor


date_word_list = ['January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November',
        'December', 'Today'
        ]


class TestSpider(CrawlSpider):
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
            process_links='filter_doubles',
            process_request='filter_moved'
                ),
             )


    def __init__(self, *args, **kwargs):
        super(TestSpider, self).__init__(*args, **kwargs)
        self.visited = []


    def start_requests(self):
        self.pages_crawled = 0
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse_first)
            

    def parse_first(self, response):
        """ Parsing method for first urls that are passed into Scrapy.
            These urls are the main board urls for bitcointalk, and thus
            filtering the urls in these responses will cut down on
            processing duplicates and unwanted links.
        """
        #response = self.filter_unwanted(response)
        return self.parse(response)


    def filter_moved(self, request):
        anchor = request.meta.get('link_text')
        if 'MOVED' in anchor:
            return None
        else:
            return request


    def filter_unwanted(self, response):
        """ Filtering method for removing the numbered pages of each
            topic, moved links, and doubled links such as 'all' or 'msg'.
        """
        soup = BeautifulSoup(response.body, "html.parser")

        smalls = soup.find_all('small')
        for s in smalls:
            s.decompose()

        moved = soup.find_all('tr', class_='ignored_topic')
        for m in moved:
            m.decompose()

        return response.replace(body=str(soup))


    def filter_doubles(self, links):
        new_links = []
        for link in links:
            if link.url not in self.visited:
                new_links.append(link)

        return new_links


    def parse_page(self, response):
        for link in LinkExtractor(allow_domains=
                self.allow_domains).extract_links(response):
            yield scrapy.Request(url=link.url,
                    callback=self.read_posts_bitcointalk)


    def parse_boards(self, response):
        self.pages_crawled += 1
        self.check_max_pages()
        response = self.filter_unwanted(response)
        self.visited.append(response.url)
        print 'parse_boards % s' % response.url
        return self.parse(response)


    def parse_posts(self, response):
        print 'parse_posts % s' % response.url


    def parse_posts_additional(self, response, dates=[], texts=[]):
        links = LinkExtractor(allow_domains=
                self.allow_domains,
                restrict_xpaths=("//a[contains(@class, 'navPages')]")
                ).extract_links(response)

        for link in links:
            print 'parse_additional % s' % link.url
            req = requests.get(url=link.url)
            soup  = BeautifulSoup(req.content, "html.parser")
            dates_new, texts_new = self.get_post_data(soup)
            [dates.append(d) for d in dates_new]
            [texts.append(t) for t in texts_new]
        return dates, texts


    def get_post_data(self, soup):
        """ Gets dates and text from SOUP, a BeautifulSoup object created
            from a bitcointalk post.
            Args:
                soup(BeautifulSoup): BeautifulSoup object
            Returns:
                dates: list of dates from post
                texts: list of text from post
        """
        texts_raw = soup.find_all('div', class_="post")
        for t in texts_raw:
            quotes = t.find_all('div', class_="quote")
            for q in quotes:
                q.decompose()
            q_header = t.find_all('div', class_="quoteheader")
            for qh in q_header:
                qh.decompose()

        sigs = soup.find_all('div', class_='signature')
        for s in sigs:
            s.decompose()

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

        return dates, texts


    def crawl_multiple_pages(self, response):
        """ Method to continue crawling a topic if it contains multiple pages.
            Recursively iterates through the topic.
        """
        pass


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

    spider = TestSpider()

    process.crawl(spider, start_urls=url,
            allow_domains=allowed_domain, max_pages=max_pages)
    process.start()
    process.stop()

    dates = []
    texts = []
    return dates, texts

forum_urls = ["https://bitcointalk.org/index.php?board=5.400"]
allowed_domains = ["bitcointalk.org",]
scrape_forums(forum_urls, allowed_domains, max_pages=25)
