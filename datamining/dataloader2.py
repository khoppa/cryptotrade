# -*- coding: utf-8 -*-
""" Creates the dataset which will contain all price data.
    The dataset will have: X = Time, Y = Coin, Z = Coin base/pair,
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

    def get_bases(self):
        """ Gets unique base coins/currencies that coins are traded against.
        """
        url = 'https://min-api.cryptocompare.com/data/all/exchanges'
        df = self.scraper.get_tradepair_data(url)
        exchanges = df.columns.values
        arr = df.as_matrix()
        idx = np.where(df.notnull())
        arr = arr[idx]
        bases = []
        for x in arr:
            for b in x:
                if b not in bases:
                    bases.append(b)
        collator = icu.Collator.createInstance(icu.Locale('de_DE.UTF-8'))
        bases = sorted(bases, key=collator.getSortKey)
        self.exchanges = exchanges
        self.bases = bases
        self.write_columns()

    def write_columns(self):
        """ Writes Bases that coins are traded against and Exchanges that
            they are traded on to files.
        """
        with open('bases','wb') as fp:
            pickle.dump(self.bases, fp)
        with open('exchanges', 'wb') as ex:
            pickle.dump(self.exchanges, ex)

    def make_prices(self):
        """ Makes Price subcolumns for the dataset.
        """
        pass

    def make_bases(self):
        """ Makes Base subcolumns for dataset.
        """
        pass

    def make_coinpairs(self):
        """ Makes CoinPair subcolumns for dataset.
        """
        pass

    def make_dataset(self):
        """ Makes Dataset.
        """
        if self.read:
            self.dataset = tables.open_file('data/data.hdf5')
        else:
            data_file = tables.open_file('data/data.hdf5', mode='w')

            coinlist = pd.read_csv('data/coinlist.csv', encoding='utf-8')
            cl = coinlist['Symbol'].values
            n_coins = len(cl)

            with open('bases', 'rb') as fp:
                bases = pickle.load(fp)
            print bases
            n_bases = len(bases)

            with open('exchanges', 'rb') as ex:
                x = pickle.load(ex)
            exchanges = [s.encode('utf-8') for s in x]
            print exchanges
            n_exchanges = len(exchanges)

            filters = tables.Filters(complevel=5, complib='blosc')
            data = data_file.create_earray(data_file.root, 'dataset',
                        tables.Atom.from_dtype(np.dtype(np.float32)),
                        shape=(0, n_coins, n_bases, 5, n_exchanges),
                        filters=filters)
            base_store = data_file.create_array(data_file.root, 'bases',
                            bases)
            exchange_store = data_file.create_array(data_file.root, 'exchanges',
                                exchanges)
            print data
            data_file.close()


class Price(IsDescription):
    """ Price nested column. Open, High, Low, Close, and Volume are recorded
        as columns, for each Exchange which is a nested column in each field.
    """

    class Open(IsDescription):
        bittrex = Float32Col()
        binance = Float32Col()

    class Close(IsDescription):
        bittrex = Float32Col()
        binance = Float32Col()


class Base(IsDescription):
    """ Substructure with Bases that Coins are traded with as rows.
    """
    BTC = Price()
    USD = Price()

def make_coinpairs():
    coinlist = pd.read_csv('data/coinlist.csv', encoding='utf-8')
    cl = coinlist['Symbol'].values[:60]
    #dictcp = {"time":Time32Col()}
    dictcp = {}
    for x in cl:
        s = x.encode('utf-8')
        dictcp[s] = Base()
    return dictcp
#dictcp = make_coinpairs()

class Data(IsDescription):
    """
    Superstructure of datset. Each row has a field TIME for time,
    and COINPAIR which is a nested Table.
    """
    
    #time = UInt32Col()
    time = Time32Col()

    class CoinPair(IsDescription):
        """
        Columns of DATA with coin names as field for columns.
        """
        
        _v_pos = 1
#Data = dictcp

"""
foo = open_file('test.h5', 'w')
table = foo.create_table(foo.root, 'table', Data)
row = table.row
row['time'] = 123456
row['CoinPair/NEO/BTC/Open/bittrex'] = 100.0
row['CoinPair/NEO/BTC/Open/binance'] = 200.0
row['CoinPair/NEO/BTC/Close/bittrex'] = 150.0
row['CoinPair/NEO/BTC/Close/binance'] = 250.0
row['CoinPair/ETH/USD/Open/bittrex'] = 300.0
row['CoinPair/ETH/USD/Open/binance'] = 400.0
row['CoinPair/ETH/USD/Close/bittrex'] = 350.0
row['CoinPair/ETH/USD/Close/binance'] = 450.0
#row['CoinPair/Base/base'] = 'USD'
#row['CoinPair/Base/Exchange/exchange'] = 'Bitstamp'
#row['CoinPair/Base/Exchange/price'] = 5000.0
#row['CoinPair/Base/Exchange/exchange'] = 'Bittrex'
#row['CoinPair/Base/Exchange/price'] = 5500.0
row.append()
table.flush()
print table[:]
foo.close()
"""

loader = Loader(read=False)
#loader.get_coinlist()
loader.get_bases()
loader.make_dataset()

"""
foo = open_file('test.hdf5', 'w')
table = foo.create_table(foo.root, 'table', Data)
print table.description
foo.close()
"""
