""" Main file which will be used to interact with datamining modules.
"""

from common import *
from dataminer import Miner 


def create_logger():
    logger = logging.getLogger('datamining')
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler('output/info.log')
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - \
            %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

def get_coin_prices(loader, major_exchanges):
    coins = loader.get_exchange_coins(major_exchanges)
    for i, e in enumerate(major_exchanges):
        ex_coins = coins[i]
        for c in ex_coins:
            logger.info('Importing prices for %s for %s' % (c, e))
            loader.make_dataset(e, c)

def get_coin_supply(loader):
    loader.import_supply()

if __name__ == "__main__":
    logger = create_logger()
    logger.info('Importing prices.')
    loader = Miner(read=False)
    #loader.get_coinlist()
    #loader.get_bases("BTC")
    major_exchanges = ["BitTrex", "Bitfinex", "Bithumb", "Bitstamp", \
            "Coinbase", "Gemini", "bitFlyer", "Binance", "Coinone", \
            "HitBTC", "Poloniex", "Kraken", "Korbit", "Huobi"]
    #major_exchanges = ["Binance"]
    #get_coin_prices(loader, major_exchanges)
    get_coin_supply(loader)
