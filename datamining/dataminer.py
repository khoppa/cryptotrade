# -*- coding: utf-8 -*-
""" Creates the dataset which will contain all price data.
    The dataset will be created for each coin in each exchange
    and will have:
    X = Time, Y = Coin base/pair, Z = OHLCV
"""
from common import *
from tables import *
from scrape import Scraper
from utils.logger import *

class Miner:
    """ Data Mining class.
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
            coinlist = df[['Symbol', 'CoinName', 'Id']]
            coinlist['Id'] = coinlist['Id'].astype(np.int64)
            coinlist.loc[:, 'pricedata'] = False
            coinlist.loc[:, 'twitter'] = []
            coinlist.loc[:, 'reddit'] = []
            coinlist.reset_index(drop=True, inplace=True)

        if self.read:
            old_cl = pd.read_csv('data/coinlist.csv', encoding='utf-8')
            if len(old_cl) < len(coinlist):
                old_cl.to_csv('data/coinlistold.csv', index=False,
                        encoding='utf-8')
                diff = pd.concat([old_cl, coinlist])
                diff = diff.reset_index(drop=True)
                diff_gp = diff.groupby(list(diff.columns)[0])
                idx = [x[0] for x in diff_gp.groups.values() if len(x) == 1]
                diff_coins = diff.reindex(idx)['Symbol']
            
                coinlist = old_cl.append(diff.iloc[idx], ignore_index=True)
                coinlist.to_csv('data/coinlist.csv', index=False,
                        encoding='utf-8')

                coinlist_logger = create_logger('datamining', 'output/coinlist.log')
                coinlist_logger.info('Added %d coins.' % len(diff_coins))
                coinlist_logger.info('Added: %s' % diff_coins.values)
                
        else:
            coinlist.to_csv('data/coinlist.csv', index=False,
                        encoding='utf-8')
        self.coinlist = coinlist

    def get_exchange_coins(self, exchanges):
        """ Gets coins that are traded on EXCHANGES. By taking an EXCHANGES
            list, narrowing the exchanges list is possible. The coins will
            be in a nested list in order of exchanges given, and fed into
            MAKE_DATASET to create the datasets.

            Args:
                exchanges (list): List of exchanges to consider
            Returns:
                coins (list): list of lists with traded coins for each
                    given exchange.
        """
        url = 'https://min-api.cryptocompare.com/data/all/exchanges'
        df = self.scraper.get_tradepair_data(url)
        coinlist = pd.read_csv('data/coinlist.csv', encoding='utf-8')
                
        #dtype={'pricedata':'boolean'})
        coins = []
        data_f = {}
        for ex in exchanges:
            ex_df = df[ex]
            ex_df.dropna(inplace=True)
            ex_coins = ex_df.index.values
            ex_coins = [s.encode('utf-8') for s in ex_coins]
            coins.append(ex_coins)
            data_f[ex] = ex_coins
            for c in ex_coins:
                coinlist.loc[coinlist.Symbol == c, 'pricedata'] = True

        with open('data/exchange_coins.txt', 'w') as f:
            json.dump(data_f, f, ensure_ascii=False)
        coinlist.to_csv('data/coinlist.csv', index=False, encoding='utf-8')
        return coins

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
            base_dict = self.check_volume(exchange, bases, coin)
            self.import_price(coin, base_dict, exchange, shape)
            
        else:
            self.get_bases(coin)
            bases = self.coin_df[exchange]

            bases = [s.encode('utf-8') for s in bases]
            bases = self.check_volume(exchange, bases, coin)
            n_bases = len(bases)
            if n_bases == 0:
                pass
            else:
                data_file = tables.open_file(coinpath, mode='w')
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
                                bases.keys())
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
            Returns:
                new_bases (dict): Dictionary of bases that COIN is traded
                    against that has volume. Keys = Base, Values = API data
                    (OHLCV)
        """
        new_bases = {}
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
                new_bases[base] = data 
        return new_bases 

    def import_price(self, coin, bases, exchange, shape):
        """ Imports price data for COIN.
            Args:
                coin (str): coin to import price data for
                bases (dict): bases coin and dataframes that it's traded against
                exchange (str): exchange coin is traded on
                shape (tuple): shape of dataset
        """
        coinpath = 'data/exchanges/{}/{}.hdf5'.format(exchange, coin) 
        data_file = tables.open_file(coinpath, mode='a')
        shape[0] = 1

        add_data = np.empty(shape, dtype=np.float32)
        add_data.fill(np.nan)

        time_data = data_file.root.time[:].tolist()
        base_keys = bases.keys()
        for b in base_keys:
            """
            url = 'https://min-api.cryptocompare.com/data/histohour?' +\
                    'fsym=' + coin +\
                    '&tsym=' + b +\
                    '&limit=2000' +\
                    '&aggregate=3' +\
                    '&e=' + exchange
            apidata = self.scraper.get_api_data(url)
            """
            apidata = bases[b]
            p_idx = base_keys.index(b)

            for i, row in apidata.iterrows():
                time = row['time']
                if time in time_data:
                    d_idx = time_data.index(time)
                    add_data[0] = data_file.root.dataset[d_idx]
                    if (~np.any(np.isnan(add_data[0, p_idx, :]))):
                        self.logger.debug('Data for %d for pair %s exists'
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

    def import_supply(self, read=False):
        """ Imports historical circulating supplies. Will do this
            based on coins wtih a .csv file in the folder.
            The method will pull data via api request from coincap.io,
            and store the timestamp, market cap, and price from the json.
        """
        coinlist = []
        for f in listdir('data/exchanges'):
            if f.endswith('.csv'):
                coinlist.append(f.split(".")[0])
        
        for c in coinlist:
            hdf5_path = 'data/exchanges/{}_supply.hdf5'.format(c)
            if read:#os.path.exists(hdf5_path):
                hdf5_file = tables.open_file(hdf5_path, mode='a')
                timedata = hdf5_file.root.dataset[:, 0]
            else:
                hdf5_file = tables.open_file(hdf5_path, mode='w')
                shape = (0, 3)

                filters = tables.Filters(complevel=5, complib='blosc')
                data = hdf5_file.create_earray(hdf5_file.root, 'dataset',
                            tables.Atom.from_dtype(np.dtype(np.int32)),
                            shape=shape,
                            filters=filters)
                timedata = []
            apidata = self.scraper.get_coincapio_data(c)
            if apidata is None:
                self.logger.info('{} has no supply data'.format(c))
                hdf5_file.close()
                continue

            else:
                for a in apidata:
                    if a[0] in timedata:
                        continue
                    else:
                        timedata.append(a[0])
                        hdf5_file.root.dataset.append([a])
                hdf5_file.close()
