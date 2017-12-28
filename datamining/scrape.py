""" Scraping module for data mining. Given a currency, will retrieve the relevant data from the cryptocompare api.
"""

import json
import requests
import pandas as pd
import numpy as np

class Scraper():
    """ Scraping class.
        Params:
    """
    def __init__(self):
        #log_path = '.'.join([folder, __name__])
        #self.logger = logging.getLogger(log_path)
        pass

    def get_api_data(self, url=''):
        #url = 'https://min-api.cryptocompare.com/data/histohour' +\
        #        '?fsym=' + self.coin +\
        #        '&tsym=' + self.base +\
        #        '&limit=2000' +\
        #        '&aggregate=1'
        response = requests.get(url).json()
        
        if response['Response'] == 'Success':
            data = response['Data']
            df = pd.DataFrame(data)
            #self.logger.info(df.head())
            return df
        else:
            #self.logger.info("API pull unsuccessful.")
            raise ValueError("API Pull unsuccessful") 
            return None

    def get_tradepair_data(self, url=''):
        """ Gets exchanges' trading pair data from CryptoCompare API.
            Seems it was implemented by a third party, and thus does not
            follow CryptoCompare's default JSON format.
        """
        response = requests.get(url).json()
        df = pd.DataFrame(response)
        return df

    def get_coincapio_data(self, coin):
        """ Gets 90-day historical data for COIN, from coincap.io.
            This returns the marketcap and coin supply time-stamped.
            Args:
                coin(str): coin to get api data for.

            Returns:
                arr(np.array): array with columns: timestamp, market cap,
                    and coin supply
                None: if no supply data exists
        """
        url = "http://coincap.io/history/90day/{}".format(coin)
        response = requests.get(url).json()
        if response == None:
            return None
        market_cap = response['market_cap']
        price = response['price']
        timestamp = [item[0] for item in market_cap]
        market_cap = [item[1] for item in market_cap]
        price = [item[1] for item in price]
        coinsupply = np.divide(market_cap, price).astype(np.int32)
        """
        d = {'timestamp': timestamp, 'market_cap': market_cap, 'price': price,
                'coinsupply': coinsupply}
        df = pd.DataFrame(d)
        """
        arr = np.array([timestamp, market_cap, coinsupply])
        return arr.transpose()
