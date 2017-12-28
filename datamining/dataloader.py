# -*- coding: utf-8 -*-
""" Creates the dataset which will contain all price data.
    The dataset will be created for each coin in each exchang
    and will have:
    X = Time, Y = Coin base/pair, Z = OHLCV
"""
from common import *
from tables import *
from scrape import Scraper
from utils.logger import *

class Loader:
    """ Data Loading class.
    """
    def __init__(self, read=False):
        log_path = '.'.join(['datamining', __name__])
        self.logger = logging.getLogger(log_path)
        self.read = read
        self.scraper = Scraper()
        self.coin_df = None
        self.coinlist = []
        self.bases = []
        self.exchanges = []

        if self.read:
            #self.logger.info("Reading datasets from file.")
            pass

        else:
            self._import()

    def _import(self):
        """ Imports data into dataset. This will be used for an initial
            creation of a dataset, and well thus call on the Scraper
            class to get data from the APIs.
        """
        pass

    def _create_dataset(self):
        data_file = tables.open_file('data/dataset.hdf5', mode='w')

    def get_coinlist(self):
        """ Gets Coin List from CryptoCompare. Will use this list to create
            columns for class CoinPair.
        """

        url = 'https://www.cryptocompare.com/api/data/coinlist'
        df = self.scraper.get_api_data(url)
        df = df.transpose()

        if df is not None:
            coinlist = df[['Symbol', 'CoinName']]
        if self.read:
            old_cl = pd.read_csv('data/coinlist.csv', encoding='utf-8')
            if len(old_cl) < len(coinlist):
                old_cl.to_csv('data/coinlistold.csv', index=False,
                        encoding='utf-8')
                coinlist.to_csv('data/coinlist.csv', index=False,
                        encoding='utf-8')
                diff = pd.concat([old_cl, coinlist])
                diff = diff.reset_index(drop=True)
                diff_gp = diff.groupby(list(diff.columns))
                idx = [x[0] for x in diff_gp.groups.values() if len(x) == 1]
                diff_coins = diff.reindex(idx)['Symbol']
            
                coinlist_logger = create_logger('datamining', 'output/coinlist.log')
                coinlist_logger.info('Added %d coins.' % len(diff_coins))
                coinlist_logger.info('Added: %s' % diff_coins.values)
                
        else:
            coinlist.to_csv('data/coinlist.csv', index=False,
                        encoding='utf-8')
        self.coinlist = coinlist

    def get_exchanges(self):
        url = 'https://min-api.cryptocompare.com/data/all/exchanges'
        df = self.scraper.get_tradepair_data(url)
        self.exchanges = df.columns.values

    def get_bases(self, coin):
        """ Gets unique base coins/currencies that COIN is traded against.
            Also gets the exchanges that said pairs are traded on.
        """
        url = 'https://min-api.cryptocompare.com/data/all/exchanges'
        df = self.scraper.get_tradepair_data(url)
        self.coin_df = df.loc[coin]
        arr = self.coin_df.as_matrix()
        idx = np.where(self.coin_df.notnull())
        self.exchanges = self.coin_df.iloc[idx].index.values
        self.exchanges = [s.encode('utf-8') for s in self.exchanges]
        arr = arr[idx]
        self.coin_df = self.coin_df.iloc[idx]
        self.coin_df = self.coin_df.reindex(self.exchanges)
        self.coin_df.to_csv('data/exchanges/{}.csv'.format(coin),
                    encoding='utf-8')

    def make_dataset(self, exchange, coin):
        """ Makes Dataset for COIN, for EXCHANGE.
            Args:
                exchange (str): String of exchange COIN is traded on.
                coin (str): String of symbol for COIN
        """
        coinpath = 'data/exchanges/{}/{}.hdf5'.format(exchange, coin) 
        if os.path.exists(coinpath):
            data_file = tables.open_file(coinpath)
            bases = data_file.root.bases[:]
            n_bases = len(bases)
            shape = [0, n_bases, 6]
            data_file.close()
            self.import_price(coin, bases, exchange, shape)
            
        else:
            data_file = tables.open_file(coinpath, mode='w')

            self.get_bases(coin)
            bases = self.coin_df[exchange]

            bases = [s.encode('utf-8') for s in bases]
            bases = self.check_volume(exchange, bases, coin)
            n_bases = len(bases)
            shape = [0, n_bases, 6]

            filters = tables.Filters(complevel=5, complib='blosc')
            data = data_file.create_earray(data_file.root, 'dataset',
                        tables.Atom.from_dtype(np.dtype(np.float32)),
                        shape=shape,
                        filters=filters)
            time_store = data_file.create_earray(data_file.root, 'time',
                            tables.Atom.from_dtype(np.dtype(np.int32)),
                            shape=(0,),
                            filters=filters)
            base_store = data_file.create_array(data_file.root, 'bases',
                            bases)
            data_file.close()
            self.import_price(coin, bases, exchange, shape)

    def check_volume(self, exchange, bases, coin):
        """ Checks volume for BASES that COIN is traded with on EXCHANGE.
            If the volume is 0 for all responses, will return bases with
            that BASE omitted.

            Args:
                exchange (str): Exchange
                bases (list): List of bases
                coin (str): Coin that is traded.
        """
        new_bases = []
        for base in bases:
            url = 'https://min-api.cryptocompare.com/data/histohour?' +\
                    'fsym=' + coin +\
                    '&tsym=' + base +\
                    '&limit=2000' +\
                    '&aggregate=3' +\
                    '&e=' + exchange
            data = self.scraper.get_api_data(url)
            if data['volumefrom'].sum() == 0 and data['volumeto'].sum() == 0:
                self.logger.info('%s for %s on %s has no volume'
                        % (base, coin, exchange))
            else:
                new_bases.append(base)
        return new_bases 


    def import_price(self, coin, bases, exchange, shape):
        """ Imports price data for COIN.
            Args:
                coin (str): coin to import price data for
                bases (list): bases coin is traded against
                exchange (str): exchange coin is traded on
                shape (tuple): shape of dataset
        """
        coinpath = 'data/exchanges/{}/{}.hdf5'.format(exchange, coin) 
        data_file = tables.open_file(coinpath, mode='a')
        shape[0] = 1

        add_data = np.empty(shape, dtype=np.float32)
        add_data.fill(np.nan)

        time_data = data_file.root.time[:].tolist()
        for b in bases:
            url = 'https://min-api.cryptocompare.com/data/histohour?' +\
                    'fsym=' + coin +\
                    '&tsym=' + b +\
                    '&limit=2000' +\
                    '&aggregate=3' +\
                    '&e=' + exchange
            apidata = self.scraper.get_api_data(url)
            p_idx = bases.index(b)

            for i, row in apidata.iterrows():
                time = row['time']
                if time in time_data:
                    d_idx = time_data.index(time)
                    add_data[0] = data_file.root.dataset[d_idx]
                    if (~np.any(np.isnan(add_data[0, p_idx, :]))):
                        self.logger.info('Data for %d for pair %s exists'
                                % (time, b))
                        continue 

                add_data[0, p_idx, 0] = row['open']
                add_data[0, p_idx, 1] = row['high']
                add_data[0, p_idx, 2] = row['low']
                add_data[0, p_idx, 3] = row['close']
                add_data[0, p_idx, 4] = row['volumefrom']
                add_data[0, p_idx, 5] = row['volumeto']

                if time in time_data:
                    data_file.root.dataset[d_idx] = add_data
                else:
                    data_file.root.time.append([time])
                    time_data.append(time)
                    data_file.root.dataset.append(add_data)

        data_file.close()
