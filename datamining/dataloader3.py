# -*- coding: utf-8 -*-
""" Creates the dataset which will contain all price data.
    The dataset will be created for each coin and will have:
    X = Time, Y = Coin base/pair,
    Zx = Exchanges, with price for XYZ for the exchange.
"""
from common import *
from tables import *
from scrape import Scraper
from utils.logger import *

class Loader:
    """ Data Loading class.
    """
    def __init__(self, read=False):
        #log_path = '.'.join([folder, __name__])
        #self.logger = logging.getLogger(log_path)
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
        bases = []
        for x in arr:
            for b in x:
                if b not in bases:
                    bases.append(b)
        collator = icu.Collator.createInstance(icu.Locale('de_DE.UTF-8'))
        bases = sorted(bases, key=collator.getSortKey)
        self.bases = [s.encode('utf-8') for s in bases]
        self.coin_df = self.coin_df.iloc[idx]
        self.coin_df = self.coin_df.reindex(self.exchanges)
        self.coin_df.to_csv('data/exchanges/{}.csv'.format(coin),
                    encoding='utf-8')

    def make_dataset(self, coin):
        """ Makes Dataset for COIN.
            Args:
                coin (str): String of symbol for COIN
        """
        coinpath = 'data/{}.hdf5'.format(coin) 
        if False:#os.path.exists(coinfile):
            dataset = tables.open_file(coinpath)
            
        else:
            data_file = tables.open_file(coinpath, mode='w')

            self.get_bases(coin)
            n_bases = len(self.bases)
            n_exchanges = len(self.exchanges)
            shape = [0, n_bases, 6, n_exchanges]

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
                            self.bases)
            exchange_store = data_file.create_array(data_file.root, 'exchanges',
                                self.exchanges)
            self.import_price(coin, shape)
            data_file.close()

    def import_price(self, coin, shape):
        """ Imports price data for COIN.
            Args:
                coin (str): coin to import price data for
                shape (tuple): shape of dataset
        """
        coinpath = 'data/{}.hdf5'.format(coin)
        data_file = tables.open_file(coinpath, mode='a')
        ex = [self.exchanges[-1]]
        shape[0] = 1

        add_data = np.empty(shape, dtype=np.float32)
        add_data.fill(np.nan)

        time_data = data_file.root.time[:].tolist()
        for e in ex:#self.exchanges:
            print e
            pairs = self.coin_df.loc[e]
            print pairs
            e_idx = self.exchanges.index(e)
            for p in pairs:
                url = 'https://min-api.cryptocompare.com/data/histohour?' +\
                        'fsym=' + coin +\
                        '&tsym=' + p +\
                        '&limit=20' +\
                        '&aggregate=3' +\
                        '&e=' + e
                apidata = self.scraper.get_api_data(url)
                print apidata

                p_idx = self.bases.index(p)
                for i, row in apidata.iterrows():
                    time = row['time']
                    if time in time_data:
                        d_idx = time_data.index(time)
                        add_data = data_file.root.dataset[d_idx]
                        assert(~np.any(~np.isnan(add_data[d_idx, p_idx, :, e_idx])))
                    else:
                        d_idx = 0

                    add_data[d_idx, p_idx, 0, e_idx] = row['open']
                    add_data[d_idx, p_idx, 1, e_idx] = row['high']
                    add_data[d_idx, p_idx, 2, e_idx] = row['low']
                    add_data[d_idx, p_idx, 3, e_idx] = row['close']
                    add_data[d_idx, p_idx, 4, e_idx] = row['volumefrom']
                    add_data[d_idx, p_idx, 5, e_idx] = row['volumeto']

                    if time in time_data:
                        data_file.root.dataset[d_idx] = add_data
                    else:
                        data_file.root.time.append([time])
                        time_data.append(time)
                        data_file.root.dataset.append(add_data)

        print data_file.root.dataset[:, p_idx, :, e_idx]
        print data_file.root.time[:]
        data_file.close()

loader = Loader(read=False)
#loader.get_coinlist()
#loader.get_bases("BTC")
loader.make_dataset("BTC")
