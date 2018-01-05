# -*- coding: utf-8 -*-
""" Dataviewing class for easy access to data.
"""
from common import *
from scrape import Scraper
from utils.logger import *

class Viewer:
    """ Data Viewing class.
    """
    def __init__(self):
        log_path = '.'.join(['datamining', __name__])
        self.logger = logging.getLogger(log_path)
        self.data_path = 'data/exchanges/'
        self.exchange_coins = json.load('data/exchange/exchange_coins.txt')

    def get_timeperiod(self, coin):
        """ Gets time period that data is available for COIN.
            
            Args:
                coin(str): Coin name to get time period for.
            Returns:
                time_period(dict): Dictionary with key = exchange,
                    value = [start time, end time]
        """
        coin_path = self.data_path + coin + '.hdf5'
        exchanges = self.get_exchanges()
        time_period = {}
        for ex in exchanges:
            coins = self.get_coinlist(ex)
            if coin in coins:
                data_path = '{}{}/{}.hdf5'.format(self.data_path,
                        ex, coin)
                data_file = tables.open_file(data_path)
                ex_time = data_file.root.time[:].sort()
                time_period[ex] = [ex_time[0], ex_time[-1]]
                data_file.close()
        return time_period

    def get_price(self, time, coin):
        """ Gets OHLCV for COIN at TIME.

            Args:
                time(int): Time to get OHLCV for.
                coin(str): Coin to get OHLCV for.
            Returns:
                ohlcv(dict): Dictionary with keys = exchanges, 
                    values = OHLCV data.
                bases(dict): Dictionary with keys = exchanges,
                    values = bases that COIN is traded against
        """
        coin_path = self.data_path + coin + '.hdf5'
        exchanges = self.get_exchanges()
        ohlcv = {}
        bases = {}
        for ex in exchanges:
            coins = self.get_coinlist(ex)
            if coin in coins:
                data_path = '{}{}/{}.hdf5'.format(self.data_path,
                        ex, coin)
                data_file = tables.open_file(data_path)
                ex_time = data_file.root.time[:]
                if time not in ex_time:
                    self.logger.info('Time does not exist for {} on \
                            {}'.format(coin, ex))
                    data_file.close()
                    continue
                time_idx = ex_time.index(time)
                ohlcv[ex] = data_file.root.dataset[time_idx]
                bases[ex] = data_file.root.bases[:]
                data_file.close()
        return ohlcv, bases

    def get_exchanges(self):
        """ Gets exchanges that data is available for.
        """
        return self.exchange_coins.keys()

    def get_coinlist(self, exchange):
        """ Gets coins that are traded on EXCHANGE.
        """
        return self.exchange_coins[exchange]

    def get_coinsupply(self, time, coin):
        """ Gets coin supply for COIN for TIME.
            Since the data source we got the coin supply only has supply
            for every 6 hours, this function will return the closest
            supply in the dataset to TIME.

            Args:
                time(int): Time in Unix epoch
                coin(str): Coin to get supply for.
            Returns:
                supply(int32): Supply of COIN at nearest TIME.
        """
        supply_path = '{}{}_supply.hdf5'.format(self.data_path, coin)
        if not os.path.exists(supply_path):
            self.logger.info('Supply for {} not available.'.format(coin))
        else:
            supply_file = tables.open_file(supply_path)
            data = supply_file.root.dataset[:]
            if len(data) == 0:
                self.logger.info('Supply for {} not available.'.format(coin))
                supply = None
            else:
                time_data = data[:, 0]
                idx = (np.abs(time_data - time)).argmin()
                supply = data[idx, 1]
            supply_file.close()
            return supply
