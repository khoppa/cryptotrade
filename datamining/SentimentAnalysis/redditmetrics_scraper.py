# -*- coding: utf-8 -*-
"""
Created on Mon Oct 16 21:29:26 2017
@author: Al
Description: simple scraper / parser that gets numbers of subreddit subscribers from 
            http://redditmetrics.com/
             
            Takes a list of subreddits as input; uses urllib to navigate to the 
            appropriate redditmetrics page; finds the number of subscriber in the html; 
            extracts and parses these data.
             
            The ScrapeRedditMetrics class can also be used on its own to get metrics 
            for a single subreddit.
             
Notes:      1. Includes a wait-time between requests to redditmetrics.com, to avoid 
            DoS'ing the website. Currently set at 2 seconds. [** WARNING **: this is 
            good practice (IMO), but if data is required for lots of subreddits, the 
            script will take a *long* time to run!!]
            2. If the intention is to query a lot of subreddits, may be best to do it
            in small chunks (e.g. 100s, rather than 1000s or more).
"""

import requests
from json import loads
import pandas as pd
from time import sleep

class ScrapeRedditMetrics():
    def __init__(self, url):
        self.url = url  
        
    def get_script_text(self):        
        """ 
        Open the web page. Print warning (and continue) if page doesn't open. 
        Return the html from the page, or None if the page doesn't open.
        """
        html = None
        try: 
            html = requests.get(self.url).content
        except Exception as e:
            print("*** WARNING: URL " + self.url + " did not open successfully. ***")
    
        return html

    def retrieve_data(self, html):
        """
        Find the part of the html with the total number of subscribers over time. 
        
        Args:
            html(str) -  a single string containing all the html in the page.
        Returns:
            A string containing all the subscriber data, or None if 
            'total-subscribers' can't be found in the html for any reason
        """
        search_string = "element: 'total-subscribers',"
        
        # In the html, the subscriber info is an array of Javascript objects (or a list 
        # of python dicts), but extracted here as a single long string.
        start_segment = html.find(search_string)
        # make sure the search string exists
        if start_segment != -1:
            start_list = html.find("[", start_segment)
            end_list = html.find("]", start_list)
            return html[start_list:end_list + 1]
        else:
            return None

    def convert_text_to_dataframe(self, data_list):
        """
        Convert the string of subscriber data to a pandas dataframe (via JSON).         
        
        Args:
            data_list(str) - a string containing the total-subscribers JSON        
        Returns:
            df(pd.DataFrame) - subscriber counts per day (as a date object)
        """
        # clean up the fields
        data_list = data_list.replace("'", '"')
        data_list = data_list.replace('a', '\"subscriber_count\"')
        data_list = data_list.replace('y', '\"date\"')
        
        # convert the string to a list of python dicts
        try:
            subscriber_data = loads(data_list)
        except ValueError:
            print("*** WARNING: No data retrieved for "+self.url+" ***")
            return None
        
        # convert to dataframe and parse dates from string to 'date'
        df = pd.DataFrame(subscriber_data)
        df['date'] = pd.to_datetime(df['date'],
                format="%Y-%m-%d").astype(int) // (10 ** 9)
        
        return df
    
    def scrape_and_parse(self):
        """
        Run all the methods above, to get the data, parse it an return the averaged 
        results. Return a dataframe with the averaged results if successful, or None 
        if unsuccessful.
        """
        # get the html
        text = self.get_script_text()
        # find the part that corresponds to total subscribers to the subreddit
        if text is not None:
            data_list = self.retrieve_data(text)
            # convert to a pandas dataframe
            if data_list is not None:
                df = self.convert_text_to_dataframe(data_list)
                return df
        else:
            return None
